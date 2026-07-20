import sys
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import col, least, greatest
from pyspark.sql.functions import mean, stddev


def compute_node_degrees(canonical_edges_df):
    src_deg = canonical_edges_df.groupBy("src").count().withColumnRenamed("src", "node").withColumnRenamed("count", "src_count") # Group the rows by source node and counts how many times it appeared as a source node
    dst_deg = canonical_edges_df.groupBy("dst").count().withColumnRenamed("dst", "node").withColumnRenamed("count", "dst_count") # Group the rows by destination node and counts how many times it appeared as destination node
    degree_df = src_deg.join(dst_deg, on="node", how="outer").na.fill(0) # performing outer join 
    return degree_df.withColumn("degree", col("src_count") + col("dst_count")).select("node", "degree") # new column called 'degree' created by adding out-degree and in-degree to get total number of edges connected to each node

def main(input_path, output_file_path):
    spark = SparkSession.builder.appName("HeavyHitterTriangles").getOrCreate()

    # Load and canonicalize edges
    raw_df = spark.read.text(input_path)
    edges_df = raw_df.select(
    regexp_extract("value", r"(\d+)[\t ]+(\d+)", 1).cast("int").alias("sorc"),
    regexp_extract("value", r"(\d+)[\t ]+(\d+)", 2).cast("int").alias("dsti"))  # read the file line by line as a single string column named 'value' , we extract 2 integers from each line(handles both tabs and spaces)  

    canonical_edges_df = edges_df.withColumn("src", least("sorc", "dsti")) \
                                 .withColumn("dst", greatest("sorc", "dsti")) \
                                 .select("src", "dst").dropDuplicates()   # to ensure each edge is stored in one direction only to remove the duplicated edges

    degree_df = compute_node_degrees(canonical_edges_df)
    num_edges = canonical_edges_df.count()
    threshold = int(math.sqrt(num_edges)) 

    # Slides based threshold calculation
    threshold_slide = int(math.sqrt(num_edges))

    # Statistical method based threshold calculation
    '''
    stats = degree_df.select(mean("degree").alias("mean_deg"), stddev("degree").alias("std_deg")).first()
    mu = stats["mean_deg"]
    mu = stats["mean_deg"]
    sigma = stats["std_deg"]
    k = 1
    threshold = int(mu + k*sigma)
    '''


    # percentile Based Threshold calculation
    '''
    quantile = 0.90
    relative_error = 0.01
    threshold = int(degree_df.approxQuantile("degree", [quantile], relative_error)[0])
    '''
    
    threshold = threshold_slide
    heavy_hitters_df = degree_df.filter(col("degree") >=threshold) # filters the degree_df to keep only nodes whose degree is >= threshold

    heavy_edges_df = canonical_edges_df \
        .join(heavy_hitters_df.select(col("node").alias("src")), on="src") \
        .join(heavy_hitters_df.select(col("node").alias("dst")), on="dst")

    edge1 = heavy_edges_df.select(col("src").alias("a"), col("dst").alias("b"))
    edge2 = heavy_edges_df.select(col("src").alias("a"), col("dst").alias("c"))

    triangle_candidates = edge1.join(edge2, on="a") \
                               .filter(col("b") < col("c")) \
                               .select("a", "b", "c")

    edge_bc = canonical_edges_df \
        .withColumn("e1", least("src", "dst")) \
        .withColumn("e2", greatest("src", "dst")) \
        .select(col("e1").alias("b"), col("e2").alias("c"))

    heavy_triangles_df = triangle_candidates.join(edge_bc, on=["b", "c"]) \
                                            .select("a", "b", "c") \
                                            .dropDuplicates()

    #  Collect and write to flat file
    triangles = heavy_triangles_df.collect()
    true_output_file_path = output_file_path
    with open(output_file_path, "w") as f:
        for row in triangles:
            f.write(f"{row['a']} {row['b']} {row['c']}\n")
    


    
    all_node_degrees = degree_df.select("node", "degree").orderBy("degree", ascending=False).collect()
    heavy_hitter_nodes = set(row['node'] for row in heavy_hitters_df.select("node").collect())
    output_summary = f"""
    Triangles written to: {output_file_path}
    Total Edges: {num_edges}
    Heavy Hitter Nodes: {len(heavy_hitter_nodes)}
    Heavy Hitter Triangles: {len(triangles)}
    Threshold value for the node to be heavy hitter : {threshold}
    All Nodes and Degrees:
    """
    for row in all_node_degrees:
         marker = "*" if row['node'] in heavy_hitter_nodes else " "
         output_summary += f"  {marker} Node {row['node']} -> Degree {row['degree']}\n"

    # Canonical Edges
    canonical_edges = canonical_edges_df.orderBy("src", "dst").collect()
    output_summary += "\nCanonical Edges:\n"
    for row in canonical_edges:
        output_summary += f"  Edge: {row['src']} -> {row['dst']}\n"

# Heavy Hitters
    output_summary += "\nHeavy Hitter Nodes (degree greater than equal to threshold):\n"
    for node in sorted(heavy_hitter_nodes):
        output_summary += f"  Node: {node}\n"

# Heavy Edges
    heavy_edges = heavy_edges_df.orderBy("src", "dst").collect()
    output_summary += "\nHeavy Edges (edges between heavy hitter nodes only):\n"
    for row in heavy_edges:
        output_summary += f"  Edge: {row['src']} -> {row['dst']}\n"

# Edge1 (a, b)
    edge1_rows = edge1.orderBy("a", "b").collect()
    output_summary += "\nEdge1 (a -> b):\n"
    for row in edge1_rows:
        output_summary += f"  a: {row['a']} -> b: {row['b']}\n"

# Edge2 (a, c)
    edge2_rows = edge2.orderBy("a", "c").collect()
    output_summary += "\nEdge2 (a -> c):\n"
    for row in edge2_rows:
        output_summary += f"  a: {row['a']} -> c: {row['c']}\n"

# Triangle Candidates
    triangle_cand_rows = triangle_candidates.orderBy("a", "b", "c").collect()
    output_summary += "\nTriangle Candidates (a, b, c):\n"
    for row in triangle_cand_rows:
        output_summary += f"  ({row['a']}, {row['b']}, {row['c']})\n"

# edge_bc (b, c)
    edge_bc_rows = edge_bc.orderBy("b", "c").collect()
    output_summary += "\nEdges for Closing Triangle (b -> c):\n"
    for row in edge_bc_rows:
        output_summary += f"  b: {row['b']} -> c: {row['c']}\n"

# Final Heavy Triangles
    output_summary += "\nHeavy Hitter Triangles:\n"
    for row in triangles:
        output_summary += f"  Triangle: ({row['a']}, {row['b']}, {row['c']})\n"     
        

    # print(output_summary)
    with open("summary_output.txt", "w") as summary_file:
        summary_file.write(output_summary)
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit heavy_hitter_triangles.py <input_file> <output_file.txt>")
        sys.exit(-1)
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    main(input_file, output_file)


