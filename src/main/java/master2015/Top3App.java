package master2015;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import com.gpjpe.TOPOLOGY_DATA_SOURCE;
import com.gpjpe.TOPOLOGY_RUN_MODE;
import com.gpjpe.TwitterTrendTopology;

public class Top3App {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        String[] params = new String[args.length + 4];
        System.arraycopy(args, 0, params, 0, args.length < 6 ? args.length : 6);
        params[params.length - 4] = "05";
        params[params.length - 3] = TOPOLOGY_RUN_MODE.REMOTE.name();
        params[params.length - 2] = TOPOLOGY_DATA_SOURCE.TWITTER.name();
        params[params.length - 1] = "false";

        TwitterTrendTopology.main(params);
    }
}
