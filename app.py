import json

import pyspark
from flask import Flask, jsonify, request
from create_dataframe import df, spark

# creating a Flask app
app = Flask(__name__)

df.createOrReplaceTempView("table")


@app.route('/', methods=['GET'])
def home():
    try:
        pdf = spark.sql("SELECT * FROM table")
        data = (pdf.select('*').rdd.flatMap(lambda x: x).collect())
        return jsonify({'data': data})
    except Exception as e:
        return {"Error": e}


@app.route('/query1', methods=['GET'])
def stock_movement():
    query1 = "select High, Low, Volume, Date, Stock, Row, ((Close - Open)/Close)*100 as Percent from table"
    pdf = spark.sql(query1)

    pdf.createOrReplaceTempView("new_table")

    query2 = "SELECT mt.Stock as min_stock, mt.Date, mt.Percent as minPerc FROM new_table mt INNER JOIN (SELECT Date, MIN(Percent) AS MinPercent FROM new_table GROUP BY " \
             "Date) t ON mt.Date = t.Date AND mt.Percent = t.MinPercent"

    query3 = "SELECT mt.Stock as max_stock, mt.Date, mt.Percent as maxPerc FROM new_table mt INNER JOIN (SELECT Date, MAX(Percent) AS MinPercent FROM new_table GROUP BY " \
             "Date) t ON mt.Date = t.Date AND mt.Percent = t.MinPercent"

    new_pdf = spark.sql(query2)
    new_pdf.createOrReplaceTempView("t1")

    new_pdf1 = spark.sql(query3)
    new_pdf1.createOrReplaceTempView("t2")
    query4 = "select t1.Date,t1.min_stock,t1.minPerc,t2.max_stock,t2.maxPerc from t1 join t2 on t1.Date = t2.Date"
    f_pdf = spark.sql(query4)
    return jsonify(json.loads(f_pdf.toPandas().to_json(orient="table", index=False)))


@app.route('/query2', methods=['GET'])
def most_traded_stock():
    try:
        query2 = "SELECT o.Date, o.Stock, o.Volume FROM `table` o LEFT JOIN `table` b ON o.Date = b.Date AND o.Volume < " \
                 "b.Volume WHERE b.Volume is NULL "
        pdf = spark.sql(query2)
        return jsonify(json.loads(pdf.toPandas().to_json(orient="table",index=False)))
    except Exception as e:
        return {"Error": e}


@app.route('/query3', methods=['GET'])
def max_min_gap():
    try:
        query = "SELECT Row, Stock, Date, Open, Close , Close- LAG(Open, 1, null) OVER (PARTITION BY Stock ORDER BY Date) " \
                "as diff FROM table "
        pdf = spark.sql(query)
        pdf.createOrReplaceTempView("new_table")
        query2 = "select Stock, MAX(diff), MIN(diff) from new_table group by Stock"
        new_pdf = spark.sql(query2)
        return jsonify(json.loads(new_pdf.toPandas().to_json(orient="table", index=False)))
    except Exception as e:
        return {"Error": e}


@app.route('/query4', methods=['GET'])
def max_movement():
    try:
        query = "select distinct Stock, abs((first_value(Open) over (partition by Stock order by Date asc) - first_value(" \
                "Close) over (partition by Stock order by Date desc))) as diff from table "
        pdf = spark.sql(query)
        pdf.createOrReplaceTempView("max_table")
        query2 = "select Stock, diff from max_table order by diff desc limit(1)"
        new_pdf = spark.sql(query2)
        return jsonify(json.loads(new_pdf.toPandas().to_json(orient="table",index=False)))
    except Exception as e:
        return {"Error": e}


@app.route("/query5")
def stddev_each_stock():
    try:
        query = "SELECT Stock, stddev(Volume) FROM table GROUP BY Stock"
        pdf = spark.sql(query)
        return jsonify(json.loads(pdf.toPandas().to_json(orient="table",index=False)))
    except Exception as e:
        return {"Error": e}


@app.route("/query6")
def mean_and_median_of_stocks():
    try:
        query = "SELECT Stock, percentile_approx(Open, 0.5) as median_open,percentile_approx(Close, " \
                "0.5) as median_close, mean(Open) as mean_of_Open, mean(Close) as mean_of_Close FROM table GROUP BY " \
                "Stock "
        pdf = spark.sql(query)
        data = pdf.select('*').rdd.flatMap(lambda x: x).collect()
        return jsonify({'Data': data})
    except Exception as e:
        return {"Error": e}


@app.route("/query7")
def avg_volume_stocks():
    try:
        query = "select Stock, AVG(Volume) as Average_of_stock from table group by Stock"
        pdf = spark.sql(query)
        return jsonify(json.loads(pdf.toPandas().to_json(orient="table",index=False)))
    except Exception as e:
        return {"Error": e}


@app.route('/query8', methods=['GET'])
def stock_higher_avg_volume():
    try:
        query8 = "SELECT Stock, AVG(Volume) FROM table GROUP BY Stock ORDER BY AVG(Volume) DESC LIMIT(1)"
        pdf = spark.sql(query8)
        data = (pdf.select('*').rdd.flatMap(lambda x: x).collect())
        return jsonify({'data': data})
    except Exception as e:
        return {"Error": e}


@app.route("/query9")
def high_low_price_stock():
    try:
        query = "select Stock, max(High) as high_price, min(Low) as low_price from table group by Stock"
        pdf = spark.sql(query)
        return jsonify(json.loads(pdf.toPandas().to_json(orient="table", index=False)))
    except Exception as e:
        return {"Error": e}


# driver function
if __name__ == '__main__':
    app.run(debug=True)
