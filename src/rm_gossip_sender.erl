%%%-------------------------------------------------------------------
%%% @author L. Bacciottini
%%% @copyright (C) 2021, UNIPI
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(rm_gossip_sender).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-include("utility.hrl").

-define(SERVER, ?MODULE).

-record(rm_gossip_sender_state, {neighbours, gossip_updates, configuration}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  {
    ok,
    #rm_gossip_sender_state{
      neighbours = [],
      gossip_updates = [],
      % default configuration value
      configuration = #config{version = 0, fanout = 4, max_neighbours = 15, sub_probability = 0.2}
      }
  }.

handle_call(_Request, _From, State = #rm_gossip_sender_state{}) ->
  {reply, ok, State}.

handle_cast(
    Request,
    State = #rm_gossip_sender_state{neighbours = Neighbours, gossip_updates = Gossips, configuration = Config}
) ->
  case Request of

    %% used at the beginning to initialize neighbours list
    {neighbours, Neigh_list} when is_list(Neigh_list) ->
      {
        noreply, % we initialize neighbours list with the list received in the cast request
        #rm_gossip_sender_state{neighbours = Neigh_list, gossip_updates = Gossips, configuration = Config}
      };

    %% used at the beginning to initialize configuration
    {configuration, New_Config} when is_record(New_Config, config) ->
      {
        noreply,
        #rm_gossip_sender_state{neighbours = Neighbours, gossip_updates = Gossips, configuration = New_Config}
      };

    {new_neighbour, Node_name} when is_atom(Node_name) ->

      Random = rand:uniform(), % to choose if the node must be added to neighbors
      if
        % if neighbours list is not full always add the node
        length(State#rm_gossip_sender_state.neighbours) <
          State#rm_gossip_sender_state.configuration#config.max_neighbours ->
          {
            noreply,
            #rm_gossip_sender_state{
              neighbours = Neighbours ++ [Node_name],
              gossip_updates = Gossips,
              configuration = Config
            }
          };
        % otherwise replace a neighbor with a certain probability
        Random < Config#config.sub_probability ->
          {
            noreply,
            #rm_gossip_sender_state{
              neighbours = substitute_rand_element(Neighbours, Node_name),
              gossip_updates = Gossips,
              configuration = Config
            }
          }
      end;

    %% used to push a management gossip
    {gossip, management, Msg} ->
      io:format("[rm_gossip_sender] received management message to gossip~n"),
      send_gossip(
        Neighbours,
        [Msg],
        Config#config.fanout
      ),
      {
        noreply,
        State
      };

    %% used to push a list of new gossip updates
    {gossip, Updates} when is_list(Updates) ->
      {
        noreply,
        #rm_gossip_sender_state{neighbours = Neighbours, gossip_updates = Gossips ++ Updates, configuration = Config}
      };

    %% used to trigger the gossip for all gossip updates in the list
    {trigger} ->
      io:format("[rm_gossip_sender] received gossip trigger~n"),
      send_gossip(
        Neighbours,
        Gossips,
        Config#config.fanout
      ),
      {
        noreply, % clear gossip updates list after gossip
        #rm_gossip_sender_state{neighbours = Neighbours, gossip_updates = [], configuration = Config}
      };

    %% catch all clause
    _ ->
      io:format("[rm_gossip_sender] WARNING: bad request format~n"),
      {noreply, State}

  end,
  {noreply, State}.

handle_info(_Info, State = #rm_gossip_sender_state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #rm_gossip_sender_state{}) ->
  ok.

code_change(_OldVsn, State = #rm_gossip_sender_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

substitute_rand_element(List, New_elem) ->
  %% Replace a random element in List with New_elem
  Already_present = lists:member(New_elem, List),
  if
    not is_list(List) -> bad_parameter;
    Already_present -> List;
    true ->
      Index = rand:uniform(length(List)),
      lists:delete(lists:nth(Index, List), List) ++ [New_elem]
  end.

extract_one(DestList, SourceList) ->
  %% extract a random element from SourceList and puts it into DestList. Return the result
  Index = rand:uniform(length(SourceList)),
  Chosen = lists:nth(Index, SourceList),
  {DestList ++ [Chosen],  lists:delete(Chosen, SourceList)}.

extract_gossip_targets_helper([], _, ResultList) ->
  ResultList;
extract_gossip_targets_helper(Neighbours, Fanout, ResultList) ->
  if
    Fanout > length(ResultList) ->
      {NewRes, NewNeigh} = extract_one(ResultList, Neighbours),
      extract_gossip_targets_helper(NewNeigh, Fanout, NewRes);
    true ->
      ResultList
  end.

extract_gossip_targets(Neighbours, Fanout) ->
  %% Return a list with Fanout random elements of Neighbours
  extract_gossip_targets_helper(Neighbours, Fanout, []).

send_gossip(Neighbours, Msg, Fanout) ->
  %% send Msg to Fanout random neighbours inside Neighbours
  Send = fun (RMNode) ->
    gen_server:cast({RMNode, rm_gossip_reception}, Msg) end,

  Targets = extract_gossip_targets(Neighbours, Fanout),

  lists:foreach(Send, Targets).


format_state(#rm_gossip_sender_state{neighbours = N, gossip_updates = G, configuration = C}) ->
  %% used for debugging, prints the internal server state
  io:format("neighbours: ~w,~ngossip_updates: ~w,~nconfiguration: { ", [N,G]),
  format_conf(C),
  io:format("}~n").


