from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, sum
import math

def find_heavy_hitter_triangles(input_file, output_file):
    # Initialize Spark session
    spark = SparkSession.builder.appName("HeavyHitterTriangles").getOrCreate()
    
    # Read the input file - handle both space and tab delimiters
    edges_df = spark.read.text(input_file)
    edges_df = edges_df.withColumn("split_values", col("value").split(r"\s+")) \
                      .select(
                          col("split_values")[0].cast("int").alias("node1"),
                          col("split_values")[1].cast("int").alias("node2")
                      ).distinct()
    
    # Calculate threshold (sqrt of number of edges)
    num_edges = edges_df.count()
    threshold = math.sqrt(num_edges)
    
    # Find heavy hitter nodes (nodes with degree >= threshold)
    # Get degrees for all nodes from both columns
    node1_degrees = edges_df.groupBy("node1").agg(count("*").alias("degree"))
    node2_degrees = edges_df.groupBy("node2").agg(count("*").alias("degree"))
    
    # Combine and find heavy hitters with proper type casting
    heavy_hitters = node1_degrees.union(node2_degrees) \
                               .groupBy("node1") \
                               .agg(sum(col("degree").cast("int")).alias("total_degree")) \
                               .filter(col("total_degree") >= threshold) \
                               .select("node1") \
                               .distinct()
    
    # Filter edges to only those between heavy hitters
    heavy_edges = edges_df.join(heavy_hitters.alias("h1"), col("node1") == col("h1.node1")) \
                         .join(heavy_hitters.alias("h2"), col("node2") == col("h2.node1")) \
                         .select("node1", "node2") \
                         .distinct()
    
    # Create bidirectional edges
    reversed_edges = heavy_edges.select(col("node2").alias("node1"), col("node1").alias("node2"))
    all_edges = heavy_edges.union(reversed_edges).distinct()
    
    # Find triangles using self-joins
    # First join: find paths of length 2 (a-b and b-c)
    paths = all_edges.alias("e1").join(
        all_edges.alias("e2"),
        col("e1.node2") == col("e2.node1")
    ).select(
        col("e1.node1").alias("a"),
        col("e1.node2").alias("b"),
        col("e2.node2").alias("c")
    ).filter("a < c")  # Avoid duplicates
    
    # Second join: check if a-c exists to complete the triangle
    triangles = paths.join(
        all_edges.alias("e3"),
        (col("a") == col("e3.node1")) & (col("c") == col("e3.node2"))
    ).select("a", "b", "c").distinct()
    
    # Write the output
    triangles.write.csv(output_file, header=False, mode="overwrite")
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: spark-submit <your-code> <input-file> <output-file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    find_heavy_hitter_triangles(input_file, output_file)