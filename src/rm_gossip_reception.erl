%%%-------------------------------------------------------------------
%%% @author L. Bacciottini
%%% @copyright (C) 2021, UNIPI
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(rm_gossip_reception).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-include("utility.hrl").

-define(SERVER, ?MODULE).

-record(rm_gossip_reception_state, {config_version}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  {ok, #rm_gossip_reception_state{config_version = 0}}.

handle_call(_Request, _From, State = #rm_gossip_reception_state{}) ->
  {reply, ok, State}.

handle_cast(Request, State = #rm_gossip_reception_state{config_version = ConfigVersion}) ->

  case Request of

    % A gossip sent by another replica manager
    {gossip, From, Gossips} when is_list(Gossips) ->
      io:format("[rm_gossip_reception] Received a new gossip~n"),
      gen_server:cast(rm_gossip_sender, {new_neighbour, From}),
      {
        noreply,
        #rm_gossip_reception_state{config_version = parse_gossips(Gossips, ConfigVersion)}
      };

    % sent by rm_map_server to notify a new configuration from a Dispatcher.
    % It has already been delivered to rm_gossip_sender by rm_map_server
    {config, NewConfig = #config{}} ->
      io:format("[rm_gossip_reception] Received a new configuration~n"),
      format_conf(NewConfig),
      {
        noreply,
        #rm_gossip_reception_state{config_version = NewConfig#config.version}
      };

    % catch all clause
    _ ->
      io:format("[rm_gossip_reception] WARNING: bad request format"),
      {noreply, State}

  end.

handle_info(_Info, State = #rm_gossip_reception_state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #rm_gossip_reception_state{}) ->
  ok.

code_change(_OldVsn, State = #rm_gossip_reception_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

parse_gossips([], ConfigVersion , [H|T]) ->
  % what remains in the third parameter are application gossip updates and are sent all together
  gen_server:cast(rm_map_server, {updates, gossip,[H|T]}),
  ConfigVersion;

parse_gossips([], ConfigVersion , []) ->
  ConfigVersion;

parse_gossips([H = {config, Configuration = #config{}}|T], ConfigVersion, Updates) ->
  if
    Configuration#config.version > ConfigVersion ->
      gen_server:cast(rm_gossip_sender, {gossip, management, H}),
      gen_server:cast(rm_gossip_sender, H),
      gen_server:cast(rm_map_server, {config, gossip, Configuration}),
      parse_gossips(T, Configuration#config.version, Updates);
    true ->
      parse_gossips(T, ConfigVersion, Updates)
  end;

parse_gossips([{topology, From, TTL}|T], ConfigVersion, Updates) ->
  gen_server:cast(rm_gossip_sender, {new_neighbour, From}),
  if
    TTL > 1 ->
      gen_server:cast(rm_gossip_sender, {gossip, management, {topology, From, TTL - 1}});
    true -> true
  end,
  parse_gossips(T, ConfigVersion, Updates);

parse_gossips([H|T], ConfigVersion, Updates) ->
  parse_gossips(T, ConfigVersion, Updates ++ [H]).

parse_gossips(Gossips, ConfigVersion) ->
  parse_gossips(Gossips, ConfigVersion, []).