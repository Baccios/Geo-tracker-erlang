%%%-------------------------------------------------------------------
%%% @author feder
%%% @copyright (C) 2021, <COMPANY>
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

-record(dispatcher_state, {neighbours_list, rms_list, configuration}).
%% neighbours_list = dispatchers
%% rms_list = replica managers list

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  {ok,
    #dispatcher_state{
      neighbours_list = [],
      rms_list = [],
      % default configuration value %%timeout in milliseconds (5000ms = 5s)
      configuration = #dispatcher_config{ rm_config = #config{version = 0, fanout = 4, max_neighbours = 8, sub_probability = 0.2}, timeout_alive = 5000 , gossip_protocol_timeout = 50}
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
      Reply = {registration_reply,
              get_neigh_list_for_new_rm(Rms_list,Config#dispatcher_config.rm_config#config.max_neighbours),
              get_TTL(length(Rms_list),Config#dispatcher_config.rm_config#config.fanout),
              Config#dispatcher_config.timeout_alive,
              Config#dispatcher_config.gossip_protocol_timeout,
              Config#dispatcher_config.rm_config
      },
      {
        reply,
        Reply,
        #dispatcher_state{neighbours_list = Neigh_list, rms_list = Rms_list ++ [{RM_id, erlang:monotonic_time(millisecond)}], configuration = Config} %%diff in time by -
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

    %% used at the beginning to initialize neighbours list of dispatchers
    {neighbours_init, New_Neigh_list} when is_list(New_Neigh_list) ->
      io:format("[dispatcher] received neighbours list: ~w~n", [New_Neigh_list]),
      {
        noreply,
        #dispatcher_state{neighbours_list = New_Neigh_list, rms_list = Rms_list, configuration = Config}
      };

    %% used to add a dispatcher id to the list of neighbours
    {neighbours_add, New_Neigh} when is_atom(New_Neigh) ->
      io:format("[dispatcher] received a new neighbour: ~w~n", [New_Neigh]),
      {
        noreply,
        #dispatcher_state{neighbours_list = Neigh_list ++ [New_Neigh], rms_list = Rms_list, configuration = Config}
      };

    %% used to remove a dispatcher id from the list of neighbours
    {neighbours_del, Neigh} when is_atom(Neigh) ->
      io:format("[dispatcher] receive order to delete neighbour: ~w~n", [Neigh]),
      {
        noreply,
        #dispatcher_state{neighbours_list = lists:delete(Neigh, Neigh_list), rms_list = Rms_list, configuration = Config}
      };



    %% catch all clause
    _ ->
      io:format("[dispatcher] WARNING: bad request format~n"),
      {noreply, State}

  end.


handle_info(_Info, State = #dispatcher_state{}) ->
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