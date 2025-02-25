import os
import sys

from flask import Flask, current_app, g, send_from_directory, request
from flask_pymongo import PyMongo

from pymongo.errors import DuplicateKeyError, OperationFailure, BulkWriteError
import bson
from bson.objectid import ObjectId
from bson.errors import InvalidId
from werkzeug.local import LocalProxy

app = Flask(__name__)
app.config['DEBUG'] = True
app.config['LOGGER_NAME'] = 'warehouse'
app.config['LOGGER_HANDLER_POLICY'] = 'always'
app.config['MONGO_URI'] = 'mongodb://root:root@localhost:27017/warehouse?authSource=admin'

def get_db():
    if 'db' not in g:
        g.db = PyMongo(app).db
        app.logger.info("Database connected")
    return g.db


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/icon')


@app.route("/warehouse")
@app.route('/warehouse/<id>', methods=['GET'])
def get_warehouse(id):
    db = get_db()
    if not id:
        return list(db.warehouse.find().sort("warehouseID", 1))
    else:
        r = db.warehouse.find_one({"warehouseID": int(id)})
        if not r:
            r =  {"message": "Not found"}
        return r

@app.route("/warehouse", methods=['POST'])
def add_warehouse():
    db = get_db()
    try:
        db.warehouse.insert_one(request.json)
        return {"message": "Warehouse added successfully"}
    except DuplicateKeyError:
        return {"message": "Warehouse already exists"}
    except OperationFailure:
        return {"message": "Invalid data"}

@app.route("/warehouse/<id>", methods=['DELETE'])
def delete_warehouse(id):
    db = get_db()
    r = db.warehouse.delete_one({"warehouseID": int(id)})
    if r.deleted_count == 0:
        return {"message": "Warehouse not found"}
    return {"message": "Warehouse deleted successfully"}

@app.route("/product")
@app.route('/product/<id>', methods=['GET'])
def get_product(id = None):
    db = get_db()
    if not id:
        return list(db.product.find().sort("productID", 1))
    else:
        r = db.product.find_one({"productID": str(id)})
        if not r:
            r =  {"message": "Not found"}
        return r

def run_product_pipe(db):

    pipeline = [
                  {
                    "$unwind": "$warehouseData"
                  },
                  {
                    "$unwind": "$warehouseData.productData"
                  },
                  {
                    "$project": {
                      "_id": "$warehouseData.productData.productID",
                      "productID":
                        "$warehouseData.productData.productID",
                      "productName":
                        "$warehouseData.productData.productName",
                      "productQuantity":
                        "$warehouseData.productData.productQuantity"
                    }
                  },
                  {
                    "$merge": {
                      "into": "product",
                      "whenMatched": "replace",
                      "whenNotMatched": "insert"
                    }
                  }
                ]
    db.warehouse.aggregate(pipeline)

@app.route("/product/<id>", methods=['POST'])
def add_product(id):
    db = get_db()
    try:
        db.warehouse.update_one({"warehouseID": int(id)}, {"$push": {"warehouseData.0.productData": request.json}})
        run_product_pipe(db)
        return {"message": "Product added successfully"}
    except DuplicateKeyError:
        return {"message": "Product already exists"}
    except OperationFailure:
        return {"message": "Invalid data"}
    except TypeError:
        return {"message": "Invalid id"}

@app.route("/product/<id>", methods=['DELETE'])
def delete_product(id):
    db = get_db()
    r = db.product.delete_one({"productID": str(id)})
    db.warehouse.update_one({"warehouseData.0.productData.productID": str(id)}, {"$pull": {"warehouseData.0.productData": {"productID": str(id)}}})
    if r.deleted_count == 0:
        return {"message": "Product not found"}
    return {"message": "Product deleted successfully"}


@app.route("/insert")
def insert_data():
    db = get_db()
    try:
        with open('warehouse.warehouse.json') as f:
            data = bson.json_util.loads(f.read())
            db.warehouse.insert_many(data)
        run_product_pipe(db)
    except BulkWriteError:
        return {"message": "Data already inserted"}
    return {"message": "Data inserted successfully"}

if __name__ == '__main__':
    print("Server started")
    sys.stdout.flush()
    app.run()


