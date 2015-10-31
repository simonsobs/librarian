#!/usr/bin/env php
<?php

// set up the Librarian DB for testing.
// Create a bunch of observations and files.
// Do this using RPCs rather than direct DB access

require_once("hera_util.inc");
require_once("test_setup.inc");
require_once("hl_rpc_client.php");
require_once("mc_rpc_client.php");

function test_setup() {
    global $test_stores;
    $pols = array('xx', 'yy', 'xy', 'yx');
    $nstores = count($test_stores);

    // make the store directories
    //
    foreach ($test_stores as $store) {
        @mkdir($store->path_prefix);
    }

    // make 10 observations
    //
    for ($i=1; $i<=10; $i++) {
        $julian_date = time() - rand(0, 100*86400);
        $polarization = $pols[rand(0, 3)];
        $length = .1*rand(0, 864000);
//        $ret = mc_create_observation($julian_date, $polarization, $length);
//        if (!$ret->success) {
//            echo "mc_create_observation() error: $ret->message\n";
//            continue;
//        }
//        $obs_id = $ret->id;
        $obs_id = $i;
        $ret = create_observation(
            TEST_SITE_NAME, $obs_id, $julian_date, $polarization, $length
        );
        if (!$ret->success) {
            echo "create_observation() error: $ret->message\n";
        }

        // for each observation, create a few files.
        // Put them on a random store.
        //
        for ($j=0; $j<4; $j++) {
            $f = "file_".$obs_id."_$j";
            $store = $test_stores[rand(0, $nstores-1)];
            if (1) {
                $path = $store->path_prefix.'/'.$f;
                $string = "aslkjsdf $i $j\n";
                file_put_contents($path, $string);
                $size = -1;
                $md5 = null;
            } else {
                $size = rand(1, 100)*1.e9;
                $md5 = random_string();
            }
            $ret = create_file(
                TEST_SITE_NAME, $store->name, $f, '', $obs_id, $size, $md5
            );
            if ($ret->success) {
                echo "created file $f\n";
            } else {
                echo "create_file() error: $ret->message\n";
                continue;
            }
        }
    }
}

test_setup();
?>
