%%%-------------------------------------------------------------------
%%% @author L. Bacciottini, F. Pacini
%%% @copyright (C) 2021, UNIPI
%%% @doc
%%%
%%% @end
%%% Created : 03. Feb 2021 12:51 PM
%%%-------------------------------------------------------------------
-author("L. Bacciottini, F.Pacini").
-record(config, {version, fanout, max_neighbours, sub_probability}).
-record(dispatcher_config, {rm_config, timeout_alive, gossip_protocol_timeout}).

format_conf(#config{version = V, fanout = F, max_neighbours = M, sub_probability = S}) ->
  io:format("version: ~w, fanout: ~w, max_neighbours = ~w, sub_probability = ~w", [V,F,M,S]).

format_dispatcher_conf(#dispatcher_config{rm_config = RM_config, timeout_alive = TA, gossip_protocol_timeout = GT}) ->
  format_conf(RM_config),
  io:format(", timeout_alive = ~w, gossip_protocol_timeout = ~w", [TA,GT]).

