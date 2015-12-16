package master2015;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import com.gpjpe.TopologyDataSource;
import com.gpjpe.TopologyRunMode;
import com.gpjpe.TwitterTrendTopology;

public class Top3App {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        String[] params = new String[args.length + 4];
        System.arraycopy(args, 0, params, 0, args.length < 6 ? args.length : 6);
        params[params.length - 4] = "05";
        params[params.length - 3] = TopologyRunMode.REMOTE.name();
        params[params.length - 2] = TopologyDataSource.KAFKA.name();
        params[params.length - 1] = "false";

        TwitterTrendTopology.main(params);
    }
}
