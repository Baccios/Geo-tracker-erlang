%%%-------------------------------------------------------------------
%%% @author feder
%%% @copyright (C) 2021, UNIPI
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(dispatcher).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-import(math,[log10/1]).

-include("utility.hrl").

-define(SERVER, ?MODULE).
-define(TIMEOUT_ALIVE,5000). %%milliseconds
-define(GOSSIP_PROTOCOL_TIMEOUT,50).

-define(INCREMENT_FANOUT_STEP,2).
-define(INCREMENT_MAX_NEIGHBOURS_STEP,5).

-record(dispatcher_state, {neighbours_list, rms_list, configuration}).
%% neighbours_list = dispatchers
%% rms_list = replica managers list

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init(Neighbours_list) ->
  %erlang:send_after(?TIMEOUT_ALIVE, dispatcher, {check_alives_timeout}), %%handled by handle_info callback
  io:format("List ~w~n", [Neighbours_list]),
  {ok,
    #dispatcher_state{
      neighbours_list = lists:delete(node(), Neighbours_list),
      rms_list = [],
      % default configuration value %%timeout in milliseconds (5000ms = 5s)
      configuration = #dispatcher_config{ rm_config = #config{version = 0, fanout = 4, max_neighbours = 8, sub_probability = 0.2}, timeout_alive = ?TIMEOUT_ALIVE , gossip_protocol_timeout = ?GOSSIP_PROTOCOL_TIMEOUT}
    }
  }.

handle_call(Request, From, State = #dispatcher_state{neighbours_list = Neigh_list, rms_list = Rms_list, configuration = Config}) -> %Synchronous request
  io:format("Call requested: Request = ~w From = ~w ~n",[Request, From]),
  format_state(State),
  case Request of
    %% used by a rm to reveal its presence
    {registration, RM_id} when is_atom(RM_id) ->
      %io:format("time : ~w" ,[erlang:monotonic_time(millisecond)]),
      io:format("[dispatcher] receive a registration mex from RM: ~w~n", [RM_id]),
      send_message(Neigh_list,{registration_propagation,RM_id}),
      Validity = check_validity_of_rm_config(Config#dispatcher_config.rm_config#config.fanout, length((Rms_list) + 1)),
      case Validity of
        bad ->
          New_version = Config#dispatcher_config.rm_config#config.version + 1,
          send_message(Neigh_list,{config_change,New_version}),
          New_rm_config = update_config(Config, New_version),
          gen_server:cast({rm_map_server, extract_n_random_element_from_list(Rms_list,1)},{config, New_rm_config#dispatcher_config.rm_config}); %%infect one rm
        _ -> New_rm_config = Config
      end,
      Reply = {registration_reply,
        get_neigh_list_for_new_rm(Rms_list,New_rm_config#dispatcher_config.rm_config#config.max_neighbours),
        get_TTL(length(Rms_list),New_rm_config#dispatcher_config.rm_config#config.fanout),
        New_rm_config#dispatcher_config.timeout_alive,
        New_rm_config#dispatcher_config.gossip_protocol_timeout,
        New_rm_config#dispatcher_config.rm_config
      },
      {
        reply,
        Reply,
        #dispatcher_state{neighbours_list = Neigh_list, rms_list = Rms_list ++ [{RM_id, erlang:monotonic_time(millisecond)}], configuration = New_rm_config} %%diff in time by -
      };

    {update, User_ID, New_state, Version, Priority} ->
      io:format("[dispatcher] receive a update mex from user: ~w~n", [User_ID]),
      RM_ID = lists:nth(1, extract_n_random_element_from_list(Rms_list,1)), %%Since extract.. returns a list (in this case of 1 element)
      Reply = gen_server:call({rm_map_server, RM_ID},{update, User_ID, New_state, Version, Priority}),
      %%Update alive
      %%Return reply
      {
        reply,
        Reply,
        #dispatcher_state{neighbours_list = Neigh_list, rms_list = renew_last_time_contact_of_rm_in_list(RM_ID,Rms_list), configuration = Config}
      };

    {map, List_of_user_id_version} ->
      io:format("[dispatcher] receive a map mex from user: ~w~n", [From]),
      RM_ID = lists:nth(1, extract_n_random_element_from_list(Rms_list,1)), %%Since extract.. returns a list (in this case of 1 element)
      Reply = gen_server:call({rm_map_server, RM_ID},{map, List_of_user_id_version}),
      %%Update alive
      %%Return reply
      {
        reply,
        Reply,
        #dispatcher_state{neighbours_list = Neigh_list, rms_list = renew_last_time_contact_of_rm_in_list(RM_ID,Rms_list), configuration = Config}
      };

    %% catch all clause
    _ ->
      io:format("[dispatcher] WARNING: bad request format~n"),
      {reply, bad_request, State}
  end.

handle_cast(Request, State = #dispatcher_state{neighbours_list = Neigh_list, rms_list = Rms_list, configuration = Config}) -> %Asynchronous request
  io:format("Cast requested: Request = ~w ~n",[Request]),
  format_state(State),
  case Request of

    %% used to add a dispatcher id to the list of neighbours
    {neighbours_add, New_Neigh} when is_atom(New_Neigh) ->
      io:format("[dispatcher] received a new neighbour: ~w~n", [New_Neigh]),
      {
        noreply,
        #dispatcher_state{neighbours_list = Neigh_list ++ [New_Neigh], rms_list = Rms_list, configuration = Config}
      };

    %% used to remove a dispatcher id from the list of neighbours
    {neighbours_del, Neigh} when is_atom(Neigh) ->
      io:format("[dispatcher] received order to delete neighbour: ~w~n", [Neigh]),
      {
        noreply,
        #dispatcher_state{neighbours_list = lists:delete(Neigh, Neigh_list), rms_list = Rms_list, configuration = Config}
      };

    {check_alives} -> %%to check which rm has not contacted the dispatcher for 3* TIMEOUT_ALIVE
      New_Rms_list = check_alives(Rms_list),
      {
        noreply,
        #dispatcher_state{neighbours_list = Neigh_list, rms_list = New_Rms_list, configuration = Config}
      };

    {alive,RM_id} when is_atom(RM_id) ->
      io:format("[dispatcher] received an alive mex from RM: ~w~n", [RM_id]),
      send_message(Neigh_list,{alive_propagation,RM_id}), %not alive otherwise ping pong effect
      {
        noreply,
        #dispatcher_state{neighbours_list = Neigh_list, rms_list = renew_last_time_contact_of_rm_in_list(RM_id,Rms_list), configuration = Config}
      };

    {alive_propagation,RM_id} when is_atom(RM_id) ->
      io:format("[dispatcher] received an alive_propagation mex from RM: ~w~n", [RM_id]),
      {
      noreply,
      #dispatcher_state{neighbours_list = Neigh_list, rms_list = renew_last_time_contact_of_rm_in_list(RM_id,Rms_list), configuration = Config}
      };

    {registration_propagation, RM_id} when is_atom(RM_id) ->
      io:format("[dispatcher] received an registration_propagation mex for RM: ~w~n", [RM_id]),
      {
        noreply,
        #dispatcher_state{neighbours_list = Neigh_list, rms_list = Rms_list ++ [{RM_id, erlang:monotonic_time(millisecond)}], configuration = Config} %%diff in time by -
      };

    {config_change, New_Version} when is_integer(New_Version) ->
      io:format("[dispatcher] received a config_change mex with version: ~w~n", [New_Version]),
      {
        noreply,
        #dispatcher_state{neighbours_list = Neigh_list, rms_list = Rms_list, configuration = update_config(Config, New_Version)}
      };

    %% catch all clause
    _ ->
      io:format("[dispatcher] WARNING: bad request format~n"),
      {noreply, State}

  end.

%%callback fro send_after, or in general all ! messages
handle_info(Info, State = #dispatcher_state{}) ->
  case Info of
    {check_alives_timeout} ->
      io:format("Timeout expired: time to check rms alives ~n"),
      gen_server:cast(dispatcher, {check_alives}),
      erlang:send_after(?TIMEOUT_ALIVE, dispatcher, {check_alives_timeout});
    _ ->
      io:format("[dispatcher] WARNING: bad mex format in handle_info~n")
  end,
  {noreply, State}.

terminate(_Reason, _State = #dispatcher_state{}) ->
  ok.

code_change(_OldVsn, State = #dispatcher_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
format_state(#dispatcher_state{neighbours_list = N, rms_list = R, configuration = C}) ->
  %% used for debugging, prints the internal server state
  io:format("**STATE**~nneighbours: ~w, ~nrms: ~w,~nconfiguration: { ", [N,R]),
  format_dispatcher_conf(C),
  io:format("}~n").


extract_n_random_element_from_list([], _) -> []; %%helper function
extract_n_random_element_from_list(_, 0) -> []; %%helper function
extract_n_random_element_from_list(List, N) -> %%helper function
  %% extract a random element from List
  Index = rand:uniform(length(List)),
  Chosen = lists:nth(Index, List),
  [element(1,Chosen)] ++ extract_n_random_element_from_list(lists:delete(Chosen, List), N - 1).

get_neigh_list_for_new_rm(Rms_list, MaxNeigh) ->
  if
    length(Rms_list) =< MaxNeigh -> [RM_id || {RM_id, _} <- Rms_list]; %%If not enough elements in RM_list -> return all
    true -> extract_n_random_element_from_list(Rms_list, MaxNeigh)
  end.

get_TTL(Number_of_nodes, Fanout) ->
  case Number_of_nodes of
    0 -> 0;
    _ -> ceil(log10(Number_of_nodes) / log10(Fanout)) %%log base: fanout argument: Number_of_nodes
  end.

check_alives(Rms_list) ->
  Now = erlang:monotonic_time(millisecond),
  [{RM_id, Last_time_contact} || {RM_id, Last_time_contact} <- Rms_list, Now - Last_time_contact < 3 * ?TIMEOUT_ALIVE].

renew_last_time_contact_of_rm_in_list(RM_id_target,Rms_list) ->
  Now = erlang:monotonic_time(millisecond),
  New_Rms_list = [{RM_id, Last_time_contact} || {RM_id, Last_time_contact} <- Rms_list, RM_id_target /= RM_id],
  New_Rms_list ++ [{RM_id_target, Now}]. %In this way, even if a dispatcher has lost a registration mex, it will add the rm_id in this occasion

send_message(_, {}) ->
  empty_message;
send_message(Neighbours, Msg) ->
  %% send Msg to all neighbours (dispatchers)
  Send = fun (DispatcherNode) ->
    % io:format("~w~n", [RMNode]) end, % DEBUG
    gen_server:cast({dispatcher, DispatcherNode}, Msg) end, %dispatcher is the name of gen_server in DispatcherNode node

  lists:foreach(Send, Neighbours).

remove_nth_element_from_list(Index, List) ->
  Target = lists:nth(Index, List),
  lists:delete(Target, List).

check_validity_of_rm_config(Fanout, Number_of_nodes) ->
  Average_cycles_gossip_protocol = ceil(log10(Number_of_nodes) / log10(Fanout)),
  if
    Average_cycles_gossip_protocol =< 3 -> ok;
    true -> bad
  end.

update_config(Config, New_Version) ->
  Current_version = Config#dispatcher_config.rm_config#config.version,
  if
    Current_version >= New_Version -> Config;

    true ->
      Gossip_protocol_timeout = Config#dispatcher_config.gossip_protocol_timeout,
      Timeout_alive = Config#dispatcher_config.timeout_alive,
      Sub_probability = Config#dispatcher_config.rm_config#config.sub_probability,
      Updated_version = Config#dispatcher_config.rm_config#config.version + 1,
      Updated_fanout = Config#dispatcher_config.rm_config#config.fanout + ?INCREMENT_FANOUT_STEP,
      Updated_max_neighbours = Config#dispatcher_config.rm_config#config.fanout + ?INCREMENT_MAX_NEIGHBOURS_STEP,

      New_rm_config = #config{version = Updated_version, fanout = Updated_fanout, max_neighbours = Updated_max_neighbours, sub_probability = Sub_probability},
      New_config = #dispatcher_config{rm_config= New_rm_config ,timeout_alive = Timeout_alive ,gossip_protocol_timeout = Gossip_protocol_timeout },
      update_config(New_config, New_Version)
  end.



