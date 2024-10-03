import os
import re
import altair as alt
import pandas as pd

# alt.renderers.enable("browser")
alt.renderers.enable("svg")


def find_subfolders_with_prefix(parent_folder, prefix):
    """
    Find all subfolders of parent_folder that start with the given prefix.
    """
    matching_subfolders = []
    for root, dirs, files in os.walk(parent_folder):
        for folder_name in dirs:
            if folder_name.startswith(prefix):
                matching_subfolders.append(os.path.join(root, folder_name))
    if len(matching_subfolders) == 0:
        folder_name = os.path.basename(os.path.normpath(parent_folder))
        if folder_name.startswith(prefix):
            matching_subfolders.append(parent_folder)

    return matching_subfolders


def get_query_tag(query_txt):
    match = re.search("--query(\d+)", query_txt)
    if match:
        query_tag = match.group(1)
    else:
        query_tag = "00"
    return query_tag


def visualize_timings(df, output_dir):
    grouped = df.groupby("scale_factor")

    for scale_factor, group in grouped:
        # Create a new column that combines engine and file_type
        group["engine_filetype"] = group["engine"] + " (" + group["file_type"] + ")"

        # Create a boolean column to check if elapsed_time_s exists
        group["has_value"] = ~group["elapsed_time_s"].isna()

        # Create the chart
        chart = (
            alt.Chart(group)
            .mark_bar()
            .encode(
                x=alt.X("engine_filetype:N", title=None),
                y=alt.Y("elapsed_time_s:Q", title="Elapsed Time (s)"),
                color="engine_filetype:N",
                column=alt.Column("query:N", title="Query", sort="ascending"),
                tooltip=["engine", "file_type", "elapsed_time_s"],
            )
            .properties(
                title=f"Scale Factor: {scale_factor}",
                width=alt.Step(20),  # Adjust the width of each bar
            )
            .configure_axis(labelAngle=45)
        )

        # Save the chart
        output_path = os.path.join(output_dir, f"timings_sf{scale_factor}.svg")
        chart.save(output_path)

        # Display the chart
        chart.display()
