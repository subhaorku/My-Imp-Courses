# import sys
# import math
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, least, greatest
# from pyspark.sql import functions as F

# # def compute_node_degrees(edges_df):
# #     src_deg = edges_df.groupBy("src").count().withColumnRenamed("src", "node").withColumnRenamed("count", "src_count")
# #     dst_deg = edges_df.groupBy("dst").count().withColumnRenamed("dst", "node").withColumnRenamed("count", "dst_count")
# #     degree_df = src_deg.join(dst_deg, on="node", how="outer").na.fill(0)
# #     return degree_df.withColumn("degree", F.col("src_count") + F.col("dst_count")).select("node", "degree")

# def compute_node_degrees(canonical_edges_df):
#     src_deg = canonical_edges_df.groupBy("src").count().withColumnRenamed("src", "node").withColumnRenamed("count", "src_count")
#     dst_deg = canonical_edges_df.groupBy("dst").count().withColumnRenamed("dst", "node").withColumnRenamed("count", "dst_count")

#     degree_df = src_deg.join(dst_deg, on="node", how="outer").na.fill(0)
#     degree_df = degree_df.withColumn("degree", col("src_count") + col("dst_count"))
#     return degree_df.select("node", "degree")


# def main(input_path, output_path):
#     spark = SparkSession.builder.appName("HeavyHitterTriangles").getOrCreate()

#     # Load and canonicalize edges
#     edges_df = spark.read.option("delimiter", " ").csv(input_path) \
#         .select(col("_c0").cast("int").alias("sorc"), col("_c1").cast("int").alias("dsti"))
    
#     canonical_edges_df = edges_df.withColumn("src", least("sorc", "dsti")) \
#                                  .withColumn("dst", greatest("sorc", "dsti")) \
#                                  .select("src", "dst").dropDuplicates()
#     print(f"Number of edges loaded: {edges_df.count()}")
#     edges_df.show(5)

#     # Compute degrees and identify heavy hitters
#     degree_df = compute_node_degrees(canonical_edges_df)
#     num_edges = canonical_edges_df.count()
#     threshold = math.sqrt(num_edges)
#     heavy_hitters_df = degree_df.filter(col("degree") > threshold)

#     # Filter edges among heavy hitters only
#     heavy_edges_df = canonical_edges_df \
#         .join(heavy_hitters_df.select(col("node").alias("src")), on="src") \
#         .join(heavy_hitters_df.select(col("node").alias("dst")), on="dst")

#     # Triangle candidate generation
#     edge1 = heavy_edges_df.select(col("src").alias("a"), col("dst").alias("b"))
#     edge2 = heavy_edges_df.select(col("src").alias("a"), col("dst").alias("c"))
    
#     triangle_candidates = edge1.join(edge2, on="a") \
#         .filter(col("b") < col("c")) \
#         .select("a", "b", "c")

#     # Confirm triangle by checking (b, c) edge
#     edge_bc = canonical_edges_df \
#         .withColumn("e1", least("src", "dst")) \
#         .withColumn("e2", greatest("src", "dst")) \
#         .select(col("e1").alias("b"), col("e2").alias("c"))

#     heavy_triangles_df = triangle_candidates.join(edge_bc, on=["b", "c"]) \
#                                             .select("a", "b", "c") \
#                                             .dropDuplicates()

#     # Save result
#     triangle_strings_df = heavy_triangles_df.select(
#         F.concat_ws(" ", col("a"), col("b"), col("c"))
#     )

#     triangle_strings_df.coalesce(1).write.mode("overwrite").text(output_path)

#     spark.stop()
#     # heavy_triangles_df.write.mode("overwrite").csv(output_path, sep=' ', header=False)
#     # spark.stop()

# if __name__ == "__main__":
#     if len(sys.argv) != 3:
#         print("Usage: spark-submit heavy_hitter_triangles.py <input_file> <output_file>")
#         sys.exit(-1)
#     input_file = sys.argv[1]
#     output_file = sys.argv[2]
#     main(input_file, output_file)


import sys
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, least, greatest

def compute_node_degrees(canonical_edges_df):
    src_deg = canonical_edges_df.groupBy("src").count().withColumnRenamed("src", "node").withColumnRenamed("count", "src_count")
    dst_deg = canonical_edges_df.groupBy("dst").count().withColumnRenamed("dst", "node").withColumnRenamed("count", "dst_count")
    degree_df = src_deg.join(dst_deg, on="node", how="outer").na.fill(0)
    degree_df = degree_df.withColumn("degree", col("src_count") + col("dst_count"))
    return degree_df.select("node", "degree")

def main(input_path, output_path):
    spark = SparkSession.builder.appName("HeavyHitterTriangles").getOrCreate()

    # Load and canonicalize edges
    edges_df = spark.read.option("delimiter", " ").csv(input_path) \
        .select(col("_c0").cast("int").alias("sorc"), col("_c1").cast("int").alias("dsti"))

    canonical_edges_df = edges_df.withColumn("src", least("sorc", "dsti")) \
                                 .withColumn("dst", greatest("sorc", "dsti")) \
                                 .select("src", "dst").dropDuplicates()

    print(f"Number of edges loaded: {canonical_edges_df.count()}")
    canonical_edges_df.show(5)

    # Compute degrees and identify heavy hitters
    degree_df = compute_node_degrees(canonical_edges_df)
    num_edges = canonical_edges_df.count()
    threshold = math.sqrt(num_edges)
    heavy_hitters_df = degree_df.filter(col("degree") > threshold)

    # Filter edges among heavy hitters only
    heavy_edges_df = canonical_edges_df \
        .join(heavy_hitters_df.select(col("node").alias("src")), on="src") \
        .join(heavy_hitters_df.select(col("node").alias("dst")), on="dst")

    # Triangle candidate generation
    edge1 = heavy_edges_df.select(col("src").alias("a"), col("dst").alias("b"))
    edge2 = heavy_edges_df.select(col("src").alias("a"), col("dst").alias("c"))

    triangle_candidates = edge1.join(edge2, on="a") \
        .filter(col("b") < col("c")) \
        .select("a", "b", "c")

    # Confirm triangle by checking (b, c) edge
    edge_bc = canonical_edges_df \
        .withColumn("e1", least("src", "dst")) \
        .withColumn("e2", greatest("src", "dst")) \
        .select(col("e1").alias("b"), col("e2").alias("c"))

    heavy_triangles_df = triangle_candidates.join(edge_bc, on=["b", "c"]) \
                                            .select("a", "b", "c") \
                                            .dropDuplicates()

    # Collect and write to flat output file
    triangles = heavy_triangles_df.collect()

    with open(output_path, 'w') as f:
        for row in triangles:
            f.write(f"{row['a']} {row['b']} {row['c']}\n")

    print(f"Triangles written to: {output_path}")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit heavy_hitter_triangles.py <input_file> <output_file>")
        sys.exit(-1)
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    main(input_file, output_file)
