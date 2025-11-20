import json, argparse, sys, os
from tabulate import tabulate

## 
# USAGE
# 1. expecting in directory: ["indices_stats.json", "indices_stats_end.json", "nodes_stats.json", "nodes_stats_end.json"
# 2. run via `python3 YourFilePathToGithubSupportAgentTools/checkForHotSpotting.py --report search --size 20`
##


NODE_ROLES = { # cdfhilmrstw
    'data_cold': 'c',
    'data': 'd',
    'data_frozen': 'f',
    'data_hot': 'h',
    'ingest': 'i',
    'ml': 'l',
    'master': 'm',
    'remote_cluster_client': 'r',
    'data_content': 's',
    'transform': 't',
    'data_warm': 'w',
    'voting_only': 'v',
    '': ''
}

def shorten_roles(data):
    if data:
        i = ''
        for r in data:
            i += NODE_ROLES[r]
        return ''.join(i)


class IngestDistribution():

    def __init__(self, size, report_type):
        self.size = size
        self.type = report_type

    def _rank_it(self,obj,key):
        obj = sorted(obj, key=lambda k: k[key], reverse=True)
        for i in range(0,len(obj)):
            obj[i] = obj[i] | {
                f"{key}.rank": i+1
            }
        return obj

    def _read_json(self, name):
        try:
            with open(os.path.join(os.getcwd() ,name), "r") as f:
                d = f.read()
        except:
            sys.exit(f"👻 error reading file: {name}")

        if (
               "root.permission_denied" in d
            or "Bad Request. Rejected" in d
            or "Server timed out after 60 seconds" in d
            or "index_closed_exception" in d
        ):
            print(f'👻 problem with file content: {name}')
            sys.exit(d)

        try:
            d = json.loads(d)
        except:
            sys.exit(f"👻 error JSON-ing file content: {name}")
        return d 

    def _report(self, summary_indices, summary_nodes, summary_shards):
        output = [["name", "id", "roles", self.type ]]
        if self.type in ["index", "search", "refresh"]:
            output[0].append(f'pool.{self.type}')
            output[0].append(f'pool.{self.type}.reject')
        
        top_indices = [x["index"] for x in summary_indices if x[f"diff.{self.type}.rank"]<=self.size]
        output[0] += top_indices
        for n in summary_nodes:
            tmp = [ n["name"], n["id"], n["roles"] ,n[f"diff.{self.type}"] ]
            if self.type in ["index", "search", "refresh"]:
                tmp.append(n[f'diff.pool.{self.type}'])
                tmp.append(n[f'diff.pool.{self.type}.reject'])
            for top_index in top_indices:
                tmp.append( sum([x[f"diff.{self.type}"] for x in summary_shards if x["index"]==top_index and x["node.id"]==n["id"] ]) )
            output.append(tmp)


        tmp = [ "+= (all)", "", "", ]
        tmp.append(sum([x[f"diff.{self.type}"] for x in summary_nodes]))
        if self.type in ["index", "search", "refresh"]:
            tmp.append(sum([x[f'diff.pool.{self.type}'] for x in summary_nodes]))
            tmp.append(sum([x[f'diff.pool.{self.type}.reject'] for x in summary_nodes]))
        for top_index in top_indices:
            tmp.append( sum([x[f"diff.{self.type}"] for x in summary_shards if x["index"]==top_index ]) )
        output.append(tmp)

        with open(f"_checkForHotSpotting_{self.type}","w+") as f:
            f.write(tabulate(output,headers="firstrow"))
            f.write("\n")
        # print(tabulate(output))

    def _write_json(self,name,data):
        with open(name,"w+") as f:
            f.write(json.dumps(data, indent=4, sort_keys=True))

    def compare_indices_stats(self, begin, end, nodes_stats):
        index_stats = []
        shard_stats = []
        
        def node_info(node_id, field):
            # print(nodes_stats[0])
            tmp = [x[field] for x in nodes_stats if x["id"]==node_id]
            if tmp!=[]:
                return tmp[0]

        def begin_shard_info(index, shard, uuid):
            tmp = [x for x in begin["indices"][index]["shards"][shard] if x["commit"]["user_data"]["history_uuid"]==uuid]
            if tmp!=[]:
                return tmp[0]

        for index, stats in end["indices"].items():
            for shard in stats["shards"]:
                for shard_in_node_end in stats["shards"][shard]:
                    uuid = shard_in_node_end["commit"]["user_data"]["history_uuid"]
                    shard_in_node_begin = begin_shard_info(index, shard, uuid)
                    tmp = {
                         "node.id": shard_in_node_end["routing"]["node"]
                        ,"primary": shard_in_node_end["routing"]["primary"] 
                        ,"shard": shard
                        ,"index": index
                        ,"uuid": uuid 

                        ,"end.index": shard_in_node_end["indexing"]["index_total"] 
                        ,"end.refresh": shard_in_node_end["refresh"]["total"] 
                        ,"end.search": shard_in_node_end["search"]["query_total"] + shard_in_node_end["search"]["fetch_total"] + shard_in_node_end["search"]["scroll_total"] + shard_in_node_end["search"]["suggest_total"] 
                        ,"end.size": shard_in_node_end["store"]["size_in_bytes"] 
                        
                        ,"begin.index": shard_in_node_begin["indexing"]["index_total"] 
                        ,"begin.refresh": shard_in_node_begin["refresh"]["total"] 
                        ,"begin.search": shard_in_node_begin["search"]["query_total"] + shard_in_node_begin["search"]["fetch_total"] + shard_in_node_begin["search"]["scroll_total"] + shard_in_node_begin["search"]["suggest_total"]  
                        ,"begin.size": shard_in_node_begin["store"]["size_in_bytes"] 
                    }
                        
                    tmp = tmp | {
                         "node.name": node_info(tmp["node.id"], "name")
                        ,"node.roles": node_info(tmp["node.id"], "roles")

                        ,"diff.index": tmp["end.index"] - tmp["begin.index"]
                        ,"diff.refresh": tmp["end.refresh"] - tmp["begin.refresh"]
                        ,"diff.search": tmp["end.search"] - tmp["begin.search"]
                        ,"diff.size": tmp["end.size"] - tmp["begin.size"] 
                    }
                    shard_stats.append(tmp)

        shard_stats = self._rank_it(shard_stats,"diff.index")
        shard_stats = self._rank_it(shard_stats,"diff.refresh")
        shard_stats = self._rank_it(shard_stats,"diff.search")
        shard_stats = self._rank_it(shard_stats,"diff.size")

        def rollup(shard_stats, index, field):
            return [ x[field] for x in shard_stats if x["index"]==index ]

        for index in list(set([x["index"] for x in shard_stats])):
            tmp = {
                 "index": index
                ,"shards": len(rollup(shard_stats, index, "shard") )
                ,"nodes.ids": len(list(set(rollup(shard_stats,index,"node.id")))) 
                ,"nodes.roles": len(list(set(rollup(shard_stats,index,"node.roles"))))

                ,"end.index": sum(rollup(shard_stats,index,"end.index"))
                ,"end.refresh": sum(rollup(shard_stats,index,"end.refresh"))
                ,"end.search": sum(rollup(shard_stats,index,"end.search"))
                ,"end.size": sum(rollup(shard_stats,index,"end.size"))

                ,"begin.index": sum(rollup(shard_stats,index,"begin.index"))
                ,"begin.refresh": sum(rollup(shard_stats,index,"begin.refresh"))
                ,"begin.search": sum(rollup(shard_stats,index,"begin.search"))
                ,"begin.size": sum(rollup(shard_stats,index,"begin.size"))

                ,"diff.index": sum(rollup(shard_stats,index,"diff.index"))
                ,"diff.refresh": sum(rollup(shard_stats,index,"diff.refresh"))
                ,"diff.search": sum(rollup(shard_stats,index,"diff.search"))
                ,"diff.size": sum(rollup(shard_stats,index,"diff.size"))
            }
            tmp = tmp | {
                 "flag.shards_across_roles": True if tmp["nodes.roles"]>1 else False
                ,"flag.shards_per_node": round( tmp["shards"] / tmp["nodes.ids"] ,2)
            }
            index_stats.append(tmp)

        index_stats = self._rank_it(index_stats,"diff.index")
        index_stats = self._rank_it(index_stats,"diff.refresh")
        index_stats = self._rank_it(index_stats,"diff.search")
        index_stats = self._rank_it(index_stats,"diff.size")
        index_stats = self._rank_it(index_stats,f"diff.{self.type}")

        return shard_stats, index_stats 

    def compare_nodes_stats(self, begin, end):
        node_stats = []
        for node, stats_end in end["nodes"].items():
            stats_begin = begin["nodes"][node]
            tmp = {
                 "id": node
                ,"roles": shorten_roles(stats_end["roles"])
                ,"ip": stats_end["ip"]
                ,"name": stats_end["name"]

                ,"end.index": stats_end["indices"]["indexing"]["index_total"]
                ,"end.refresh": stats_end["indices"]["refresh"]["total"] 
                ,"end.search": stats_end["indices"]["search"]["query_total"] + stats_end["indices"]["search"]["fetch_total"] + stats_end["indices"]["search"]["scroll_total"] + stats_end["indices"]["search"]["suggest_total"] 
                ,"end.size": stats_end["indices"]["store"]["size_in_bytes"] 
                ,"end.shards": stats_end["indices"]["shard_stats"]["total_count"]
                ,"end.uptime": stats_end["jvm"]["uptime_in_millis"]
                ,"end.pool.index": stats_end["thread_pool"]["write"]["completed"] + stats_end["thread_pool"]["system_write"]["completed"] + stats_end["thread_pool"]["system_critical_write"]["completed"]
                ,"end.pool.search": stats_end["thread_pool"]["search"]["completed"] + stats_end["thread_pool"]["system_read"]["completed"] + stats_end["thread_pool"]["system_critical_read"]["completed"] + stats_end["thread_pool"]["search_worker"]["completed"] + stats_end["thread_pool"]["search_coordination"]["completed"] + stats_end["thread_pool"]["esql"]["completed"] + stats_end["thread_pool"]["auto_complete"]["completed"] 
                ,"end.pool.refresh": stats_end["thread_pool"]["refresh"]["completed"]
                ,"end.pool.index.reject": stats_end["thread_pool"]["write"]["rejected"] + stats_end["thread_pool"]["system_write"]["rejected"] + stats_end["thread_pool"]["system_critical_write"]["rejected"]
                ,"end.pool.search.reject": stats_end["thread_pool"]["search"]["rejected"] + stats_end["thread_pool"]["system_read"]["rejected"] + stats_end["thread_pool"]["system_critical_read"]["rejected"] + stats_end["thread_pool"]["search_worker"]["rejected"] + stats_end["thread_pool"]["search_coordination"]["rejected"] + stats_end["thread_pool"]["esql"]["rejected"] + stats_end["thread_pool"]["auto_complete"]["rejected"] 
                ,"end.pool.refresh.reject": stats_end["thread_pool"]["refresh"]["rejected"]

                ,"begin.index": stats_begin["indices"]["indexing"]["index_total"]
                ,"begin.refresh": stats_begin["indices"]["refresh"]["total"] 
                ,"begin.search": stats_begin["indices"]["search"]["query_total"] + stats_begin["indices"]["search"]["fetch_total"] + stats_begin["indices"]["search"]["scroll_total"] + stats_begin["indices"]["search"]["suggest_total"] 
                ,"begin.size": stats_begin["indices"]["store"]["size_in_bytes"] 
                ,"begin.shards": stats_begin["indices"]["shard_stats"]["total_count"]
                ,"begin.uptime": stats_begin["jvm"]["uptime_in_millis"]
                ,"begin.pool.index": stats_begin["thread_pool"]["write"]["completed"] + stats_begin["thread_pool"]["system_write"]["completed"] + stats_begin["thread_pool"]["system_critical_write"]["completed"]
                ,"begin.pool.search": stats_begin["thread_pool"]["search"]["completed"] + stats_begin["thread_pool"]["system_read"]["completed"] + stats_begin["thread_pool"]["system_critical_read"]["completed"] + stats_begin["thread_pool"]["search_worker"]["completed"] + stats_begin["thread_pool"]["search_coordination"]["completed"] + stats_begin["thread_pool"]["esql"]["completed"] + stats_begin["thread_pool"]["auto_complete"]["completed"] 
                ,"begin.pool.refresh": stats_begin["thread_pool"]["refresh"]["completed"]
                ,"begin.pool.index.reject": stats_begin["thread_pool"]["write"]["rejected"] + stats_begin["thread_pool"]["system_write"]["rejected"] + stats_begin["thread_pool"]["system_critical_write"]["rejected"]
                ,"begin.pool.search.reject": stats_begin["thread_pool"]["search"]["rejected"] + stats_begin["thread_pool"]["system_read"]["rejected"] + stats_begin["thread_pool"]["system_critical_read"]["rejected"] + stats_begin["thread_pool"]["search_worker"]["rejected"] + stats_begin["thread_pool"]["search_coordination"]["rejected"] + stats_begin["thread_pool"]["esql"]["rejected"] + stats_begin["thread_pool"]["auto_complete"]["rejected"] 
                ,"begin.pool.refresh.reject": stats_begin["thread_pool"]["refresh"]["rejected"]
            }

            tmp = tmp | {
                 "diff.index": tmp["end.index"] - tmp["begin.index"]
                ,"diff.refresh": tmp["end.refresh"] - tmp["begin.refresh"]
                ,"diff.search": tmp["end.search"] - tmp["begin.search"]
                ,"diff.size": tmp["end.size"] - tmp["begin.size"]
                ,"diff.shards": tmp["end.shards"] - tmp["begin.shards"]
                ,"diff.uptime": tmp["end.uptime"] - tmp["begin.uptime"]
                ,"diff.pool.index": tmp["end.pool.index"] - tmp["begin.pool.index"]
                ,"diff.pool.search": tmp["end.pool.search"] - tmp["begin.pool.search"]
                ,"diff.pool.refresh": tmp["end.pool.refresh"] - tmp["begin.pool.refresh"]
                ,"diff.pool.index.reject": tmp["end.pool.index.reject"] - tmp["begin.pool.index.reject"]
                ,"diff.pool.search.reject": tmp["end.pool.search.reject"] - tmp["begin.pool.search.reject"]
                ,"diff.pool.refresh.reject": tmp["end.pool.refresh.reject"] - tmp["begin.pool.refresh.reject"]
            }
            tmp = tmp | {
                 "flag.uptime": tmp["diff.uptime"]<=0
                ,"flag.index_not_hot": True if tmp["diff.index"]>0 and ("c" in tmp["roles"] or "w" in tmp["roles"]) and not ("h" in tmp["roles"] or "s" in tmp["roles"]) else False 
                ,"flag.pool_index_not_hot": True if tmp["diff.pool.index"]>0 and ("c" in tmp["roles"] or "w" in tmp["roles"]) and not ("h" in tmp["roles"] or "s" in tmp["roles"]) else False 
            }
            node_stats.append(tmp)

        node_stats = self._rank_it(node_stats,"diff.index")
        node_stats = self._rank_it(node_stats,"diff.pool.index")
        node_stats = self._rank_it(node_stats,"diff.refresh")
        node_stats = self._rank_it(node_stats,"diff.search")
        node_stats = self._rank_it(node_stats,"diff.size")
        node_stats = self._rank_it(node_stats,f"diff.{self.type}")

        return node_stats

    def compare(self):
        indices_stats_begin = self._read_json("indices_stats.json")
        indices_stats_end = self._read_json("indices_stats_end.json")
        nodes_stats_begin = self._read_json("nodes_stats.json")
        nodes_stats_end = self._read_json("nodes_stats_end.json")

        summary_nodes = self.compare_nodes_stats(nodes_stats_begin, nodes_stats_end)
        summary_shards, summary_indices = self.compare_indices_stats(indices_stats_begin, indices_stats_end, [ {"id":x["id"], "roles": x["roles"],"name": x["name"]} for x in summary_nodes] )
        
        self._write_json("_checkForHotSpotting_nodes.json", summary_nodes)
        self._write_json("_checkForHotSpotting_indices.json", summary_indices)
        self._write_json("_checkForHotSpotting_shards.json", summary_shards)
        self._report(summary_indices, summary_nodes, summary_shards)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check for hot spotting along one of the [index, refresh, search, size] vectors.')
    parser.add_argument(
        '--size','-s'
        ,dest='size'
        ,default=10
        ,type=int
    )
    parser.add_argument(
        '--report','-r'
        ,dest='report'
        ,default="index"
        ,choices=['index','refresh','search','size']
    )
    args = parser.parse_args()

    # print('👀 expecting: ["indices_stats.json", "indices_stats_end.json", "nodes_stats.json", "nodes_stats_end.json"]')
    for i in ["indices_stats.json", "indices_stats_end.json", "nodes_stats.json", "nodes_stats_end.json"]:
        if not os.path.exists(i):
            sys.exit(f'👻 missing: {i}')

    IngestDistribution(args.size, args.report).compare()
