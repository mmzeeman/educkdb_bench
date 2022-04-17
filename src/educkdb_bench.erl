
-module(educkdb_bench).

-export([main/0, appender/0, ets/0]).

ets() ->
    Ref = ets:new(bench, [ordered_set, {keypos, 1}]),

    Values = lists:seq(1, 10),
    Range = lists:seq(1, 100_000),

    {Time, _} = timer:tc(fun() ->
                                 lists:foreach(fun(I) ->
                                                       ets:insert(Ref, list_to_tuple([I] ++ Values))
                                               end,
                                               Range)
                         end),

    io:fwrite("100_000 inserts took: ~p milliseconds~n", [Time/1000]),

    %% Select 100_000 
    {SelectTime, Rows} = timer:tc(fun() ->
                                          ets:tab2list(Ref)
                                  end),

    io:fwrite("Select of ~p rows took: ~p milliseconds~n", [length(Rows), SelectTime/1000]),
 
    ok.

appender() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, []} = q(Conn, "create table bench (a int, b int, c int, d int, e int, f int, g int, h int, i int, j int)"),

    Values = lists:seq(1, 10),
    Range = lists:seq(1, 100_000),

    %% Insert 100_000 records
    {Time, _} = timer:tc(fun() ->
                                 {ok, []} = educkdb:squery(Conn, "begin;"),
                                 {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"bench">>),
                                 lists:foreach(fun(_) ->
                                                       [ ok = educkdb:append_int32(Appender, V) || V <- Values ],
                                                       ok = educkdb:appender_end_row(Appender)
                                               end,
                                               Range),
                                 educkdb:appender_flush(Appender),
                                 {ok, []} = educkdb:squery(Conn, "commit;")
                         end),

    io:fwrite("100_000 inserts took: ~p milliseconds~n", [Time/1000]),

    %% Select 100_000 
    {SelectTime, Data} = timer:tc(fun() ->
                                          {ok, R} = educkdb:query(Conn, "select * from bench"),
                                          Chunks = educkdb:get_chunks(R),
                                          [ educkdb:extract_chunk(C) || C <- Chunks ]
                                  end),
    
    NrChunks = length(Data),
    NrRows = lists:sum([ begin [ #{ data := C } | _ ] = D, length(C) end || D <- Data ]),

    io:fwrite("Select of ~p chunks and ~p rows took: ~p milliseconds~n", [NrChunks, NrRows, SelectTime/1000]),

    ok.


main() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, []} = q(Conn, "create table bench (a int, b int, c int, d int, e int, f int, g int, h int, i int, j int)"),

    Values = lists:seq(1, 10),
    Range = lists:seq(1, 100_000),

    %% Insert 100_000 records
    {Time, _} = timer:tc(fun() ->
                                 {ok, []} = q(Conn, "begin;"),

                                 {ok, Stmt} = educkdb:prepare(Conn, "insert into bench values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"),
                                 lists:foreach(fun(_) ->
                                                       [ ok = educkdb:bind_int32(Stmt, V, V) || V <- Values ],
                                                       {ok, _} = educkdb:execute_prepared(Stmt)
                                               end,
                                               Range),

                                 {ok, []} = q(Conn, "commit;")
                         end),

    io:fwrite("100_000 inserts took: ~p milliseconds~n", [Time/1000]),

    %% Select 100_000 
    {SelectTime, Data} = timer:tc(fun() ->
                                          {ok, R} = educkdb:query(Conn, "select * from bench"),
                                          Chunks = educkdb:get_chunks(R),
                                          [ educkdb:extract_chunk(C) || C <- Chunks ]
                                  end),
    
    NrChunks = length(Data),
    NrRows = lists:sum([ begin [ #{ data := C } | _ ] = D, length(C) end || D <- Data ]),

    io:fwrite("Select of ~p chunks and ~p rows took: ~p milliseconds~n", [NrChunks, NrRows, SelectTime/1000]),



    ok.


q(Conn, Query) -> educkdb:squery(Conn, Query).

x(Stmt) -> educkdb:execute(Stmt). 

