import sys
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, least, greatest, mean, stddev

def compute_node_degrees(canonical_edges_df):
    src_deg = canonical_edges_df.groupBy("src").count().withColumnRenamed("src", "node").withColumnRenamed("count", "src_count")
    dst_deg = canonical_edges_df.groupBy("dst").count().withColumnRenamed("dst", "node").withColumnRenamed("count", "dst_count")
    degree_df = src_deg.join(dst_deg, on="node", how="outer").na.fill(0)
    return degree_df.withColumn("degree", col("src_count") + col("dst_count")).select("node", "degree")

def main(input_path, output_file_path):
    spark = SparkSession.builder.appName("HeavyHitterTriangles").getOrCreate()

    # Load and canonicalize edges
    edges_df = spark.read.option("delimiter", " ").csv(input_path) \
        .select(col("_c0").cast("int").alias("sorc"), col("_c1").cast("int").alias("dsti"))

    canonical_edges_df = edges_df.withColumn("src", least("sorc", "dsti")) \
                                 .withColumn("dst", greatest("sorc", "dsti")) \
                                 .select("src", "dst").dropDuplicates()

    degree_df = compute_node_degrees(canonical_edges_df)
    num_edges = canonical_edges_df.count()

    # Threshold by percentile (most robust)
    stats = degree_df.select(mean("degree").alias("mean_deg"), stddev("degree").alias("std_deg")).first()
    mu = stats["mean_deg"]
    sigma = stats["std_deg"]
    k = 1
    threshold_stat = int(mu + k * sigma)

    quantile = 0.90
    relative_error = 0.01
    threshold_percentile = int(degree_df.approxQuantile("degree", [quantile], relative_error)[0])

    # Final threshold (can switch strategy here)
    threshold = int(math.sqrt(num_edges)) 

    heavy_hitters_df = degree_df.filter(col("degree") > threshold)

    heavy_edges_df = canonical_edges_df \
        .join(heavy_hitters_df.select(col("node").alias("src")), on="src") \
        .join(heavy_hitters_df.select(col("node").alias("dst")), on="dst")

    edge1 = heavy_edges_df.select(col("src").alias("a"), col("dst").alias("b"))
    edge2 = heavy_edges_df.select(col("src").alias("a"), col("dst").alias("c"))

    triangle_candidates = edge1.join(edge2, on="a") \
                               .filter(col("b") < col("c")) \
                               .select("a", "b", "c")

    # 🛠️ Add reverse edges to make edge list undirected for final triangle verification
    reverse_edges_df = canonical_edges_df.select(
        col("dst").alias("src"),
        col("src").alias("dst")
    )

    undirected_edges_df = canonical_edges_df.union(reverse_edges_df).dropDuplicates()

    edge_bc = undirected_edges_df \
        .withColumn("e1", least("src", "dst")) \
        .withColumn("e2", greatest("src", "dst")) \
        .select(col("e1").alias("b"), col("e2").alias("c"))

    heavy_triangles_df = triangle_candidates.join(edge_bc, on=["b", "c"]) \
                                            .select("a", "b", "c") \
                                            .dropDuplicates()

    # 🔥 Collect and write to flat file
    triangles = heavy_triangles_df.collect()
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
