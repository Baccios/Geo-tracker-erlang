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
  % default configuration value
  Config = get_default_config(),
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
        {ok, NewVersion},
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
        RequirementsAreMet ->
          {
            reply,
            {ok, ets:tab2list(Table)},
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
%% Return false if requirements are not met.
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
  io:format("[rm_map_server] WARNING: bad format in a map request"),
  false.


%% handle all pending requests. Return the list of requests that still could not be handled.
handle_pending_requests(PendingRequests, Table) ->
  handle_pending_requests(PendingRequests, Table, []).

handle_pending_requests([], _Table, NotHandled) ->
  NotHandled;

handle_pending_requests([H = {From, {map, ConsistentUsers}}|T], Table, NotHandled) ->
  RequirementsAreMet = check_versions(ConsistentUsers, Table),
  if
    RequirementsAreMet ->
      gen_server:reply(From, {ok, ets:tab2list(Table)}),
      handle_pending_requests(T, Table, NotHandled);
    true ->
      handle_pending_requests(T, Table, NotHandled ++ [H])
  end;

handle_pending_requests([_H|T], Table, NotHandled) ->
  io:format("[rm_map_server] WARNING: bad format for a pending request"),
  handle_pending_requests(T, Table, NotHandled).















