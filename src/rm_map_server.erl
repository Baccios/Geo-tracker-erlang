%%%-------------------------------------------------------------------
%%% @author L. Bacciottini
%%% @copyright (C) 2021, UNIPI
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(rm_map_server).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-include("utility.hrl").

-define(SERVER, ?MODULE).

-record(rm_map_server_state, {table_id, dispatchers, configuration, pending_requests}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init(Dispatchers) ->
  Table = ets:new(geo_map, []),
  % initial configuration value, either sent by a dispatcher or default
  Config = handle_registration_reply(register(Dispatchers)),
  {
    ok,
    #rm_map_server_state{table_id = Table, dispatchers = Dispatchers, configuration = Config, pending_requests = []}
  }.

handle_call(
    Request, From,
    State = #rm_map_server_state{
      table_id = Table, dispatchers = Dispatchers, configuration = Config, pending_requests = Pending
    }) ->

  case Request of

    % sent by a Dispatcher. It updates a user's state
    {update, UserID, Version, NewState, Priority} when is_integer(Priority) and Priority > 0 ->
      AlreadyPresent = ets:member(Table, UserID),
      if
        AlreadyPresent ->
          CurrentVersion = ets:lookup_element(Table, UserID, 2);
        true ->
          CurrentVersion = 0
      end,
      NewVersion = max(CurrentVersion, Version) + Priority,
      ets:insert(Table, {UserID, NewVersion, NewState}),
      gen_server:cast(rm_gossip_sender, {gossip, [{update, UserID, Version, NewState}]}),
      {
        reply,
        {update_reply, NewVersion},
        State
      };

    % sent by a Dispatcher to update the configuration
    % Now the new config must also be sent to rm_gossip_sender and rm_gossip_reception
    {config, NewConfig=#config{}} ->
      if
        NewConfig#config.version > Config#config.version ->
          gen_server:cast(rm_gossip_reception, Request),
          gen_server:cast(rm_gossip_sender, Request),
          gen_server:cast(rm_gossip_sender, {gossip, management, Request}),
          {
            reply,
            ok,
            #rm_map_server_state{
              table_id = Table, dispatchers = Dispatchers, configuration = NewConfig, pending_requests = Pending
            }
          };
        true ->
          {reply, old_version, State}
      end;

    % sent by a Dispatcher. It asks for a map with certain version requirements
    {map, ConsistentUsers} ->
      RequirementsAreMet = check_versions(ConsistentUsers, Table),
      if
        RequirementsAreMet == bad_format ->
          {
            reply,
            bad_format,
            State
          };
        RequirementsAreMet == true ->
          {
            reply,
            {map_reply, ets:tab2list(Table)},
            State
          };
        true ->
          {
            noreply,
            #rm_map_server_state{
              table_id = Table, dispatchers = Dispatchers, configuration = Config,
              pending_requests = Pending ++ [{From, Request}]
            }
          }
      end;

    % catch all clause
    _ ->
      io:format("[rm_map_server] WARNING: bad call request format"),
      {reply, bad_format, State}

  end.


handle_cast(
    Request,
    State = #rm_map_server_state{
      table_id = Table, dispatchers = Dispatchers, configuration = Config, pending_requests = Pending
    }) ->

  case Request of
    % sent by rm_gossip_reception when a new gossip updates list arrives
    {updates, gossip, Updates} when is_list(Updates) ->
      gen_server:cast(rm_gossip_sender, {gossip, handle_updates(Updates, Table)}),
      {
        noreply,
        #rm_map_server_state{
          table_id = Table, dispatchers = Dispatchers, configuration = Config,
          pending_requests = handle_pending_requests(Pending, Table)  % try to handle again pending requests
        }
      };

    % sent by rm_gossip_reception when a config gossip arrives.
    % It has already been delivered to rm_gossip_sender by rm_gossip_reception. thus, only update
    % the current configuration.
    {config, gossip, NewConfig=#config{}} ->
      io:format("[rm_map_server] received new configuration from gossip"),
      {
        noreply,
        #rm_map_server_state{
          table_id = Table, dispatchers = Dispatchers, configuration = NewConfig, pending_requests = Pending
        }
      };

    % sent by a Dispatcher to update the configuration
    % Now the new config must also be sent to rm_gossip_sender and rm_gossip_reception
    {config, NewConfig=#config{}} ->
      if
        NewConfig#config.version > Config#config.version ->
          gen_server:cast(rm_gossip_reception, Request),
          gen_server:cast(rm_gossip_sender, Request),
          gen_server:cast(rm_gossip_sender, {gossip, management, Request}),
          {
            noreply,
            #rm_map_server_state{
              table_id = Table, dispatchers = Dispatchers, configuration = NewConfig, pending_requests = Pending
            }
          };
        true ->
          {noreply, State}
      end;

    % catch all clause
    _ ->
      io:format("[rm_map_server] WARNING: bad cast request format"),
      {noreply, State}

  end.

handle_info({alive, NextTimeout}, State = #rm_map_server_state{dispatchers = [H|T]}) ->
  % send the periodic alive message to a random dispatcher
  ChosenDispatcher = pick_random([H|T]),
  catch gen_server:cast({dispatcher, ChosenDispatcher}, {alive, node()}), % if dispatcher is unavailable do not fail
  erlang:send_after(NextTimeout, rm_map_server, {alive, NextTimeout}),
  {noreply, State};

handle_info(_Info, State = #rm_map_server_state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #rm_map_server_state{}) ->
  ok.

code_change(_OldVsn, State = #rm_map_server_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Handle all updates in Updates and return the subset of them that must be gossiped again
handle_updates([], _, Gossips) ->
  Gossips;

handle_updates([H = {update, UserID, Version, NewState}|T], Table, Gossips) ->
  Present = ets:member(Table, UserID),
  if
    Present ->
      CurrentVersion = ets:lookup_element(Table, UserID, 2),
      if
        CurrentVersion < Version ->
          ets:insert(Table, {UserID, Version, NewState}),
          handle_updates(T, Table, Gossips ++ [H]);
        true -> handle_updates(T, Table, Gossips)
      end;
    true ->
      ets:insert_new(Table, {UserID, Version, NewState}),
      handle_updates(T, Table, Gossips ++ [H])
  end;

handle_updates([_H|T], Table, Gossips) ->
  io:format("[rm_map_server] WARNING: skipping a bad formed update~n"),
  handle_updates(T, Table, Gossips).

handle_updates(Updates, Table) ->
  handle_updates(Updates, Table, []).


%% Check the geo map given a list of required {UserID, Version}.
%% Return false if requirements are not met, bad_format is the list is bad formed.
check_versions([], _Table) ->
  true;

check_versions([{UserID, Version}|T], Table) ->
  Present = ets:member(Table, UserID),
  if
    not Present -> false;
    true ->
      CurrentVersion = ets:lookup_element(Table, UserID, 2),
      if
        CurrentVersion < Version -> false;
        true -> check_versions(T, Table)
      end
  end;

check_versions([_|_], _) ->
  io:format("[rm_map_server] WARNING: bad format in a map request~n"),
  bad_format.


%% handle all pending requests. Return the list of requests that still could not be handled.
handle_pending_requests(PendingRequests, Table) ->
  handle_pending_requests(PendingRequests, Table, []).

handle_pending_requests([], _Table, NotHandled) ->
  NotHandled;

handle_pending_requests([H = {From, {map, ConsistentUsers}}|T], Table, NotHandled) ->
  RequirementsAreMet = check_versions(ConsistentUsers, Table),
  if
    RequirementsAreMet ->
      gen_server:reply(From, {map_reply, ets:tab2list(Table)}),
      handle_pending_requests(T, Table, NotHandled);
    true ->
      handle_pending_requests(T, Table, NotHandled ++ [H])
  end;

handle_pending_requests([_H|T], Table, NotHandled) ->
  io:format("[rm_map_server] WARNING: bad format for a pending request~n"),
  handle_pending_requests(T, Table, NotHandled).

% try to register at a given dispatcher node
try_registration(Node) ->
  catch gen_server:call({dispatcher, Node}, {registration, node()}).

%pick a random element from List
pick_random(List) ->
  RandomIndex = rand:uniform(length(List)),
  lists:nth(RandomIndex, List).

% Register the current node to a random Dispatcher.
register([]) ->
  false;
register(Dispatchers) ->
  Chosen = pick_random(Dispatchers),
  Result = try_registration(Chosen),
  case Result of
    {timeout, _Description}  -> % if call fails retry with another dispatcher
      register(lists:delete(Chosen, Dispatchers));
    {'EXIT', _} -> % dispatcher does not exist
      register(lists:delete(Chosen, Dispatchers));
    _Response ->
      Result
end.

handle_registration_reply(
    {registration_reply, Neighbours, TTL, AliveTimeout, GossipTimeout, Config = #config{}}) ->

  % initialize rm_gossip_sender
  gen_server:cast(rm_gossip_sender, {gossip_timeout, GossipTimeout}),
  gen_server:cast(rm_gossip_sender, {gossip, management, {topology, node(), TTL}}),
  gen_server:cast(rm_gossip_sender, {neighbours, Neighbours}),
  gen_server:cast(rm_gossip_sender, {config, Config}),

  % initialize rm_gossip_reception
  gen_server:cast(rm_gossip_reception, {config, Config}),

  % initialize alive timeout
  erlang:send_after(AliveTimeout, rm_map_server, {alive, AliveTimeout}),

  % return the initial configuration
  io:format("[rm_map_server] Correctly registered at a dispatcher~n"),
  Config;

handle_registration_reply(false) ->
  io:format("[rm_map_server] Could not contact any dispatcher. Using default configuration~n"),
  get_default_config();

handle_registration_reply(_) ->
  io:format("[rm_map_server] The dispatcher reply was bad formed. Using default configuration~n"),
  get_default_config().











