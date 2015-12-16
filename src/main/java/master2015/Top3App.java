package master2015;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import com.gpjpe.TwitterTrendTopology;

public class Top3App {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        String[] params = new String[args.length + 2];
        System.arraycopy(args, 0, params, 0, args.length);
        params[params.length - 2] = "05";
        params[params.length - 1] = TwitterTrendTopology.TOPOLOGY_RUN_MODE.REMOTE.name();
        TwitterTrendTopology.main(params);
    }
}
