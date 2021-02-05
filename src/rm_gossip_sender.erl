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

-record(rm_gossip_sender_state, {neighbours, gossip_updates, configuration, timeout}).

-define(DEFAULT_TIMEOUT, 10000). % ten seconds

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  erlang:send_after(?DEFAULT_TIMEOUT, rm_gossip_sender, trigger),
  {
    ok,
    #rm_gossip_sender_state {
      neighbours = [],
      gossip_updates = [],
      % default configuration value
      configuration = get_default_config(),
      timeout = ?DEFAULT_TIMEOUT % default value
    }
  }.

handle_call(_Request, _From, State = #rm_gossip_sender_state{}) ->
  {reply, ok, State}.

handle_cast(
    Request,
    State = #rm_gossip_sender_state{
      neighbours = Neighbours, gossip_updates = Gossips, configuration = Config, timeout = Timeout
    }
) ->
  case Request of

    %% used at the beginning to initialize neighbours list
    {neighbours, Neigh_list} when is_list(Neigh_list) ->
      io:format("[rm_gossip_sender] received neighbours list: ~w~n", [Neigh_list]),
      {
        noreply, % we initialize neighbours list with the list received in the cast request
        #rm_gossip_sender_state{
          neighbours = Neigh_list, gossip_updates = Gossips, configuration = Config, timeout = Timeout
        }
      };

    %% used at the beginning to initialize configuration and later to update it
    {config, New_Config} when is_record(New_Config, config) ->
      io:format("[rm_gossip_sender] Received a new configuration~n"),
      format_conf(New_Config),
      {
        noreply,
        #rm_gossip_sender_state{
          neighbours = Neighbours, gossip_updates = Gossips, configuration = New_Config, timeout = Timeout
        }
      };

    {new_neighbour, Node_name} when is_atom(Node_name)  ->
      io:format("[rm_gossip_sender] received new possible neighbour: ~w~n", [Node_name]),

      Random = rand:uniform(), % to choose if the node must be added to neighbors
      if
        % if the possible neighbour is this node ignore the request
        Node_name == node() ->
          {noreply, State};

        % if neighbours list is not full always add the node (if not already present)
        length(Neighbours) <
          Config#config.max_neighbours ->
          {
            noreply,
            #rm_gossip_sender_state{
              neighbours = lists:delete(Node_name, Neighbours) ++ [Node_name],
              gossip_updates = Gossips,
              configuration = Config,
              timeout = Timeout
            }
          };

        % otherwise replace a neighbor with a certain probability
        Random < Config#config.sub_probability ->
          {
            noreply,
            #rm_gossip_sender_state{
              neighbours = substitute_rand_element(Neighbours, Node_name),
              gossip_updates = Gossips,
              configuration = Config,
              timeout = Timeout
            }
          };

        true -> {noreply, State}
      end;

    %% used to push a management gossip
    {gossip, management, Msg} ->
      io:format("[rm_gossip_sender] received management message to gossip~n"),
      send_gossip(
        Neighbours,
        {gossip, node(), [Msg]},
        Config#config.fanout
      ),
      % format_state(State),  % DEBUG
      {
        noreply,
        State
      };

    %% used to push a list of new gossip updates
    {gossip, Updates} when is_list(Updates) ->
      {
        noreply,
        #rm_gossip_sender_state{
          neighbours = Neighbours, gossip_updates = Gossips ++ Updates, configuration = Config, timeout = Timeout
        }
      };

    %% used to trigger the gossip for all gossip updates in the list
    {trigger} ->
      % io:format("[rm_gossip_sender] received gossip trigger~n"), % DEBUG
      send_gossip(
        Neighbours,
        {gossip, node(), Gossips},
        Config#config.fanout
      ),
      % format_state(State), % DEBUG
      {
        noreply, % clear gossip updates list after gossip
        #rm_gossip_sender_state{
          neighbours = Neighbours, gossip_updates = [], configuration = Config, timeout = Timeout
        }
      };

    %% specifies the new gossip timeout (valid from the next trigger)
    {gossip_timeout, NewTimeout} when NewTimeout > 0 ->
      io:format("gossip timeout period set to ~w~n", [NewTimeout]),
      {
        noreply,
        #rm_gossip_sender_state{
          neighbours = Neighbours, gossip_updates = [], configuration = Config, timeout = NewTimeout
        }
      };

    %% catch all clause
    _ ->
      io:format("[rm_gossip_sender] WARNING: bad request format~n"),
      {noreply, State}

  end.

% used to trigger the periodic gossip
handle_info(trigger, State = #rm_gossip_sender_state{}) ->
  gen_server:cast(rm_gossip_sender, {trigger}),
  erlang:send_after(State#rm_gossip_sender_state.timeout, rm_gossip_sender, trigger),
  {noreply, State};
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

send_gossip(_, {gossip, _From, []}, _) ->
  empty_gossip;
send_gossip(Neighbours, GossipMsg, Fanout) ->
  %% send Msg to Fanout random neighbours inside Neighbours
  Send = fun (RMNode) ->
    % io:format("~w~n", [RMNode]) end, % DEBUG
    gen_server:cast({rm_gossip_reception, RMNode}, GossipMsg) end,

  Targets = extract_gossip_targets(Neighbours, Fanout),

  lists:foreach(Send, Targets).


format_state(#rm_gossip_sender_state{neighbours = N, gossip_updates = G, configuration = C}) ->
  %% used for debugging, prints the internal server state
  io:format("neighbours: ~w,~ngossip_updates: ~w,~nconfiguration: { ", [N,G]),
  format_conf(C),
  io:format("}~n").


