package com.anjali.distkv;

import com.anjali.distkv.cli.DistKvCli;
import com.anjali.distkv.server.Server;

/**
 * Unified entry point for DistKV.
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && "cli".equalsIgnoreCase(args[0])) {
            String[] cliArgs = new String[args.length - 1];
            System.arraycopy(args, 1, cliArgs, 0, cliArgs.length);
            DistKvCli.main(cliArgs);
        } else {
            Server.main(args);
        }
    }
}
