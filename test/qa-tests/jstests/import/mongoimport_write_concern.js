(function() {

    load("jstests/configs/replset_28.config.js");

    var name = 'import_write_concern';

    var toolTest = new ToolTest(name, null);

    var dbName = "foo";
    var colName = "bar";

    rs = new ReplSetTest({
        name: name,
        nodes: 3,
        useHostName: true
    });

    var fileTarget = "wc.csv"

    rs.startSet({});

    rs.initiate();

    rs.awaitReplication();

    toolTest.master = rs.getMaster();

    jsTest.log("testing that mongoimport deals with write concern");

    function testWriteConcern(exitCode,writeConcern,name) {
        jsTest.log(name);
        ret = toolTest.runTool.apply(
            toolTest,
            ['import','--file',fileTarget, '-d', dbName, '-c', colName].
                concat(writeConcern).
                concat(commonToolArgs)
        );
        assert.eq(exitCode, ret, name);
        db.dropDatabase();
    }

    var member1 = rs.nodes[1].getDB("admin");
    var member2 = rs.nodes[2].getDB("admin");

    var commonToolArgs = getCommonToolArguments();

    var db = rs.nodes[0].getDB(dbName);
    var col = db.getCollection(colName);

    // create a test collection
    for(var i=0;i<=100;i++){
      col.insert({_id:i, x:i*i});
    }

    // export the data that we'll use
    var ret = toolTest.runTool.apply(
        toolTest,
        ['export','--out',fileTarget, '-d', dbName, '-c', colName].
            concat(commonToolArgs)
    );
    assert.eq(0, ret);

    // drop the database so it's empty
    db.dropDatabase();

    testWriteConcern(0, [], "restore without write concern to a fully functioning repl-set should succeed");

    testWriteConcern(0, ['--writeConcern=majority'], "restore with majority to a fully functioning repl-set should succeed");

    testWriteConcern(0, ['--writeConcern={w:1,wtimeout:1000}'], "restore with w:1,timeout:1000 to a fully functioning repl-set should succeed");

    testWriteConcern(0, ['--writeConcern={w:2,wtimeout:1000}'], "restore with w:2,timeout:1000 to a fully functioning repl-set should succeed");

    jsTest.log("stopping one node from doing any further syncing");
    member1.runCommand({configureFailPoint: 'rsSyncApplyStop', mode: 'alwaysOn'});

    testWriteConcern(0, ['--writeConcern={w:2,wtimeout:1000}'], "restore with w:2,timeout:1000 repl-set with 2 working nodes should succeed");

    testWriteConcern(0, ['--writeConcern=majority'], "restore with majority with one working node should succeed");

    testWriteConcern(1, ['--writeConcern={w:3,wtimeout:1000}'], "restore with w:3,timeout:1000 repl-set with two working nodes should fail");

    jsTest.log("stopping the other slave");
    member2.runCommand({configureFailPoint: 'rsSyncApplyStop', mode: 'alwaysOn'});

    testWriteConcern(1, ['--writeConcern={w:"majority",wtimeout:1000}'], "restore with majority with no working nodes should fail");

    testWriteConcern(1, ['--writeConcern={w:2,wtimeout:1000}'], "restore with w:2,timeout:1000 to a fully functioning repl-set should succeed");

    testWriteConcern(0, ['--writeConcern={w:1,wtimeout:1000}'], "restore with w:1,timeout:1000 repl-set with one working nodes should succeed");

    jsTest.log("restore with w:3 concern and no working slaves and no timeout waits until slaves are available");
    pid = startMongoProgramNoConnect.apply(null,
        ['mongoimport','--writeConcern={w:3}','--host',rs.nodes[0].host,'--file',fileTarget].
            concat(commonToolArgs)
    );

    sleep(1000);

    assert(checkProgram(pid), "restore with w:3 and no working slaves should not have finished");

    jsTest.log("starting stopped slaves");

    member1.runCommand({configureFailPoint: 'rsSyncApplyStop', mode: 'off'});
    member2.runCommand({configureFailPoint: 'rsSyncApplyStop', mode: 'off'});

    jsTest.log("waiting for restore to finish");
    ret = waitProgram(pid);
    assert.eq(0, ret, "restore with w:3 should succeed once enough slaves start working");

    db.dropDatabase();

    rs.stopSet();
    toolTest.stop();

}());
