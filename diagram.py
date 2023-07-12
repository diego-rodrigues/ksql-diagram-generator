from diagrams import Diagram, Cluster, Edge
from diagrams.digitalocean.compute import Droplet
from diagrams.digitalocean.database import DbaasPrimary
from diagrams.elastic.elasticsearch import LogStash

def unzip(iterable):
    return zip(*iterable)

with Diagram("My Diagram: Droplets", show=False, filename="my-diagram", direction="LR"):
    
    with Cluster("Digital Ocean"):
        d1 = Droplet("Droplet 1")
        d2 = Droplet("Droplet 2")
        c = [d1,
            d2
        ]
        # c = [Droplet("Droplet 1"),Droplet("Droplet 2")]

    # db = DbaasPrimary("My Database")
    # logstash = LogStash("Logstash service")

    # c >> db
    # db >> c

    # c >> Edge(color="firebrick", style="dashed") >> logstash
    # db >> Edge(color="firebrick", style="dashed") >> logstash

# READ KSQL

# CREATE TREE

# DRAW TREE