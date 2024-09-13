import math
from datetime import datetime
import os
import pandas as pd
import numpy as np

from Analysis.crash_ML_model import crash_model
from database.DatabaseUtil import DatabaseUtil
from database.models import Crash, Person, Vehicle
from sqlalchemy import create_engine, MetaData, Table, tuple_, or_, and_

from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.io import output_notebook, output_file, save, export_png
from bokeh.palettes import Category20_20, Category10_5, Spectral11, Category10, Category20c
from bokeh.transform import dodge, cumsum, factor_cmap
from bokeh.layouts import gridplot

from dagster import op, Out, In, get_dagster_logger

from database.DagsterDataFrames import CrashDataFrame, VehicleDataFrame, PersonDataFrame
from database.DatabaseUtil import DatabaseUtil
from database.IO_ops import IO_ops
from database.models import map_crash_columns, Crash

logger = get_dagster_logger()
output_file("visualizations.html")
height=450
width=500

def get_merged_data():
    session = DatabaseUtil.get_postgres_session()
    # sq1 = session.query(Crash)
    # crash = pd.read_sql(sq1.statement, session.bind)
    sq = session.query(Crash, Person, Vehicle) \
        .join(Person, Person.collision_id == Crash.collision_id) \
        .join(Vehicle, and_(Vehicle.collision_id == Crash.collision_id, Vehicle.vehicle_id == Person.vehicle_id))
    df = pd.read_sql(sq.statement, session.bind)
    DatabaseUtil.close_postgres_session(session)

    df.drop(columns={'collision_id_1', 'crash_time_1', 'crash_date_1', 'collision_id_2', 'crash_time_2',
                     'crash_date_2', 'vehicle_id_1'}, inplace=True)
    df['crash_time'] = df['crash_time'].astype(str).astype('datetime64[ns]').dt.time
    df['crash_date'] = df['crash_date'].astype(str).astype('datetime64[ns]')

    df['deaths_by_vehicle_make'] = df.groupby(['vehicle_make']).persons_killed.transform('sum')
    df['deaths_by_vehicle_type'] = df.groupby(['vehicle_type']).persons_killed.transform('sum')
    # df['deaths_by_borough'] = df.groupby(['borough']).persons_killed.transform('sum')

    df.replace({np.nan: None}, inplace=True)
    return df


# get_merged_data()


@op(
    ins={"vehicle_df": In(VehicleDataFrame), "crash_df": In(CrashDataFrame), "person_df": In(PersonDataFrame)},
    out=Out(bool)
)
def visualize_run_model(vehicle_df, crash_df, person_df) -> bool:
    result = True
    try:
        crash = crash_df
        vehicle = vehicle_df
        person = person_df
        df = get_merged_data()

        df.head()
        crash['deaths_by_borough'] = crash.groupby(['borough']).persons_killed.transform('sum')
        df = pd.merge(left=df, right=crash[['borough', 'deaths_by_borough']].drop_duplicates(), how='left',
                      on=['borough'])

        # Plot 1: Persons injured per crash date
        p1 = figure(x_axis_type='datetime', title='Persons Injured per Crash Date')
        p1.vbar(x=df['crash_date'], top=df['persons_injured'], width=0.5)
        p1.xaxis.axis_label = 'Crash Date'
        p1.yaxis.axis_label = 'Persons Injured'

        # Plot 2: Vehicle type distribution
        vehicle_counts = df['vehicle_type_code_1'].value_counts()
        vehicle_source = ColumnDataSource(data=dict(vehicle_types=vehicle_counts.index, counts=vehicle_counts.values))
        p2 = figure(x_range=vehicle_counts.index.tolist(), title='Vehicle Type Distribution', toolbar_location=None,
                    tools="")
        p2.vbar(x='vehicle_types', top='counts', width=0.5, source=vehicle_source, line_color='white')
        p2.xaxis.major_label_orientation = 'vertical'
        p2.xaxis.axis_label = 'Vehicle Type'
        p2.yaxis.axis_label = 'Count'
        # show(p2)

        # Create a ColumnDataSource
        df1 = df.drop_duplicates(['vehicle_type']).sort_values('deaths_by_vehicle_type', ascending=False).head(10)
        source = ColumnDataSource(data={'makes': df1['vehicle_type'], 'killed_counts': df1['deaths_by_vehicle_type']})

        # Create the figure
        p4 = figure(x_range=df1['vehicle_type'], title='Persons Killed by Vehicle Type', toolbar_location=None,
                    tools='')

        # Add vbar glyph
        p4.vbar(x='makes', top='killed_counts', width=0.9, legend_field='makes', source=source)

        # Set axis labels and orientation
        p4.xgrid.grid_line_color = None
        p4.xaxis.axis_label = 'Vehicle Type'
        p4.yaxis.axis_label = 'Number of Persons Killed'
        p4.xaxis.major_label_orientation = 1.2
        # show(p)

        # Group by borough and calculate the total number of crashes in each borough
        borough_crash_counts = crash.groupby('borough')['persons_killed'].sum().reset_index()

        ## Create the pie chart
        p5 = figure(title="Distribution of Crashes by Borough", toolbar_location=None, tools="hover",
                    tooltips="@borough: @persons_killed", x_range=(-1, 1.0), height=height, width=width)
        # Define colors for the pie slices
        borough_crash_counts['color'] = Category20c[len(borough_crash_counts)]
        borough_crash_counts['angle'] = borough_crash_counts['persons_killed'] / borough_crash_counts[
            'persons_killed'].sum() * 2 * math.pi

        # Add wedge glyphs for each borough
        p5.wedge(x=0, y=1, radius=0.4, start_angle=cumsum('angle', include_zero=True), end_angle=cumsum('angle'),
                 line_color="white",
                 fill_color='color', legend_field="borough", source=borough_crash_counts)

        # Remove grid and axis
        p5.axis.axis_label = None
        p5.axis.visible = False
        p5.grid.grid_line_color = None

        # Group by borough and calculate the total number of crashes in each borough
        borough_crash_counts = df.groupby('person_sex')['persons_killed'].sum().reset_index()

        ## Create the pie chart
        p6 = figure(title="Distribution of Crashes by Gender", toolbar_location=None, tools="hover",
                    tooltips="@person_sex: @persons_killed", x_range=(-1, 1.0), height=height, width=width)
        # Define colors for the pie slices
        borough_crash_counts['color'] = Category20c[len(borough_crash_counts)]
        borough_crash_counts['angle'] = borough_crash_counts['persons_killed'] / borough_crash_counts[
            'persons_killed'].sum() * 2 * math.pi

        # Add wedge glyphs for each borough
        p6.wedge(x=0, y=1, radius=0.4, start_angle=cumsum('angle', include_zero=True), end_angle=cumsum('angle'),
                 line_color="white",
                 fill_color='color', legend_field="person_sex", source=borough_crash_counts)

        # Remove grid and axis
        p6.axis.axis_label = None
        p6.axis.visible = False
        p6.grid.grid_line_color = None

        # Combine contributing factors from both columns
        # graph1
        df1 = df
        all_contributing_factors = pd.concat(
            [df1['contributing_factor_vehicle_1'], df1['contributing_factor_vehicle_2']], ignore_index=True)

        # Get value counts of contributing factors
        contributing_factor_counts = all_contributing_factors.value_counts().head(10)

        # Create Bokeh plot
        output_file("contributing_factors.html")

        p7 = figure(x_range=contributing_factor_counts.index.tolist(),
                    title="Top 10 Most Common Contributing Factors in Crashes",
                    x_axis_label='Contributing Factor', y_axis_label='Crash Frequency',
                    height=height, width=width)

        p7.vbar(x=contributing_factor_counts.index.tolist(), top=contributing_factor_counts.values, width=0.9
                , legend_label='Count')

        p7.xgrid.grid_line_color = None
        p7.y_range.start = 0
        p7.xaxis.major_label_orientation = 0.8  # Rotate x-axis labels for better visibility

        # graph2
        df2 = crash
        df2['crash_date'] = df2['crash_date'].astype(str).astype('datetime64[ns]')
        now = datetime.now()
        ten_years_ago = now.replace(year=now.year - 10)
        recent_accidents = df2[df2['crash_date'] >= ten_years_ago]
        accidents_per_year = recent_accidents['crash_date'].dt.year.value_counts().sort_index()

        # Create Bokeh plot
        output_file('accidents_analysis.html')

        # Plot: Number of accidents per year in the last 10 years
        years = accidents_per_year.index.astype(str).tolist()
        num_accidents = accidents_per_year.values.tolist()

        p8 = figure(title='Number of Accidents per Year in the Last 5 Years', x_range=years,
                    y_range=(0, max(num_accidents) + 10))
        p8.vbar(x=years, top=num_accidents, width=0.5)

        # Graph 3 : Bar chart of vehicle_make
        make_counts = df1['vehicle_make'].value_counts().head(10)

        source = ColumnDataSource(data=dict(vehicle_make=make_counts.index.tolist(), counts=make_counts.tolist()))
        p9 = figure(x_range=make_counts.index.tolist(), title="Top 10 Vehicle Makes Involved in Collisions",
                    x_axis_label='Vehicle Make', y_axis_label='Count')
        p9.vbar(x='vehicle_make', top='counts', width=0.9, source=source)
        p9.xaxis.major_label_orientation = 0.8

        # GRaph 4
        # Extract day of the week from crash_date
        df1['day_of_week'] = df1['crash_date'].dt.day_name()

        # Count the occurrences of accidents by day of the week
        day_counts = df1['day_of_week'].value_counts().reindex(
            ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])

        # Create Bokeh plot
        output_file("accidents_day_of_week.html")

        days = day_counts.index.tolist()
        counts = day_counts.values.tolist()
        # colors = Category10[len(days)]  # Color palette for bars

        p10 = figure(x_range=days, title="Accidents by Day of Week",
                     x_axis_label='Day of Week', y_axis_label='Number of Accidents', height=height)

        # Add bars to the plot
        p10.vbar(x=days, top=counts, width=0.9)

        # Rotate x-axis labels for better visibility
        p10.xaxis.major_label_orientation = 1.2

        # Graph 6

        df1['person_age'] = pd.to_numeric(df1['person_age'])

        p12 = figure(title="Histogram of Age of People Involved in Accidents", tools="hover",
                     tooltips=[("Age", "@left"), ("Count", "@top")], x_range=(0, 100))

        hist, edges = np.histogram(df1['person_age'], bins=10, range=(0, 100))  # Specify range and bins

        p12.quad(top=hist, bottom=0, left=edges[:-1], right=edges[1:], alpha=0.7)  # line_color="white",

        # Customize plot properties
        p12.xaxis.axis_label = "Age"
        p12.yaxis.axis_label = "Count"

        # Graph 5
        # Define time of day categories
        def get_time_category(time_str):
            hour = int(time_str.split(':')[0])
            if 5 <= hour < 12:
                return 'Morning'
            elif 12 <= hour < 17:
                return 'Afternoon'
            elif 17 <= hour < 21:
                return 'Evening'
            else:
                return 'Night'

        # Assign time of day categories
        df1['crash_time'] = df1['crash_time'].astype(str)
        df1['time_category'] = df1['crash_time'].apply(get_time_category)

        # Aggregate accidents counts by time category
        time_category_counts = df1.groupby('time_category')['collision_id'].sum().reindex(
            ['Morning', 'Afternoon', 'Evening', 'Night'])

        time_categories = time_category_counts.index.tolist()
        accident_counts = time_category_counts.values.tolist()

        # Create bar chart
        p11 = figure(x_range=time_categories, title="Accidents by Time of Day", height=height, width=width,
                     toolbar_location=None, tools="hover", tooltips="@time_categories: @accident_counts")

        p11.vbar(x=time_categories, top=accident_counts, width=0.9)

        # Customize plot properties
        p11.xgrid.grid_line_color = None
        p11.y_range.start = 0
        p11.xaxis.axis_label = "Time of Day"
        p11.yaxis.axis_label = "Number of Accidents"

        # Graph 7
        region_counts = df1['borough'].value_counts()  # Change to 'state' for state-level analysis

        # Convert data to ColumnDataSource
        source = ColumnDataSource(data=dict(region=region_counts.index.tolist(), counts=region_counts.values.tolist()))

        # Create figure
        p13 = figure(x_range=source.data['region'], height=height, title="Accidents by Region",
                     toolbar_location=None, tools="")

        # Plot bars
        p13.vbar(x='region', top='counts', width=0.9, source=source, line_color='white')

        # Customize plot properties
        p13.xgrid.grid_line_color = None
        p13.y_range.start = 0
        p13.xaxis.axis_label = "Region"
        p13.yaxis.axis_label = "Number of Accidents"
        p13.xaxis.major_label_orientation = 1.2

        # graph 8

        # Aggregate collisions by driver_sex
        collision_counts = df1['driver_sex'].value_counts()

        # Create Bokeh plot
        # output_file("bar_collisions_by_driver_sex.html")

        # Convert data to ColumnDataSource
        source = ColumnDataSource(
            data=dict(driver_sex=collision_counts.index.tolist(), counts=collision_counts.values.tolist()))

        # Create figure
        p14 = figure(x_range=source.data['driver_sex'], height=height, title="Collisions by Driver Sex",
                     toolbar_location=None, tools="")

        p14.vbar(x='driver_sex', top='counts', width=0.9, source=source)

        # Customize plot properties
        p14.xgrid.grid_line_color = None
        p14.y_range.start = 0
        p14.yaxis.axis_label = "Number of Collisions"
        p14.xaxis.axis_label = "Driver Sex"

        X_train, X_test, y_train, y_test, mse, predictions = crash_model()
        # Calculate the residuals
        residuals = y_test.values.flatten() - predictions.flatten()

        # Create QQ plot for residuals
        residuals = y_test.values - predictions
        qqplot_source = ColumnDataSource(data=dict(residuals=residuals, theoretical=predictions))
        qqplot = figure(title='QQ Plot of Residuals', outer_width=600, outer_height=400)
        qqplot.scatter('theoretical', 'residuals', source=qqplot_source, size=7, color='navy', alpha=0.5)
        qqplot.line([-3, 3], [-3, 3], line_color="red", line_width=2, legend_label="y=x")
        qqplot.xaxis.axis_label = 'Theoretical quantiles'
        qqplot.yaxis.axis_label = 'Sample quantiles'
        qqplot.legend.location = 'top_left'

        # p15
        # Calculate the range for the plot
        min_val = min(np.min(y_test), np.min(predictions))
        max_val = max(np.max(y_test), np.max(predictions))
        p15 = figure(title='Y-test vs Predicted', x_axis_label='Y-test', y_axis_label='Predicted',
                     width=500, height=400, x_range=(min_val, max_val), y_range=(min_val, max_val))

        # Add a scatter plot of y-test vs predicted values
        source = ColumnDataSource(data=dict(x=y_test, y=predictions))
        p15.circle('x', 'y', source=source, size=10, color='navy', alpha=0.6)

        # Add a diagonal line for reference
        p15.line([min_val, max_val], [min_val, max_val], line_width=2, line_color="red")

        show(gridplot(
            [
                [p1, p2, p14],
                [p4, p5, p6],
                [p7, p8, p9],
                [p10, p11, p12],
                [p13, qqplot, p15]
            ],
            width=width, height=height))

        # Create output folder if it doesn't exist
        cwd = os.getcwd()
        output_folder = os.path.join(cwd, 'dashboard_folder').replace("\\", '/')
        # output_folder = "images_folder"
        os.makedirs(output_folder, exist_ok=True)

        graphs = [p1, p2, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, qqplot]
        # Create and save multiple plots
        for idx, p in enumerate(graphs, start=1):
            # Save plot as HTML
            output_file(os.path.join(output_folder, f"plot_{idx}.html"))
            save(p)
            # Save plot as PNG
            # export_png(p, filename=os.path.join(output_folder, f"plot_{idx}.png"))
        result = True
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False

    return result









