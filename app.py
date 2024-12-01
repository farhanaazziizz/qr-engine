from flask import Flask, request, jsonify
import requests
import uuid
import redis
from datetime import datetime
import pytz
import os


app = Flask(__name__)

REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')
REDIS_DB = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True
)

pdf_source = '/pdf_source/'
pdf_stemped = '/pdf_stemped/'
created_at = datetime.now(pytz.timezone(
    'Asia/Jakarta')).strftime('%Y-%m-%d %H:%M:%S')
qr_id = str(uuid.uuid4())


def download_file(id, pdf_url):
    try:
        response = requests.get(pdf_url)
        output_path = f"{pdf_source}{created_at}_{id}.pdf"
        if response.status_code == 200:
            with open(output_path, "wb") as f:
                f.write(response.content)
        else:
            raise Exception(
                f"Failed to download file. Status code: {response.status_code}")
    except Exception as e:
        raise RuntimeError(f"Failed to download PDF. Error: {e}")


def insert_to_redis(id, qr_url, pdf_url, qr_position_x, qr_position_y, api_callback):
    data_key = f"data:{qr_id}"
    REDIS_DB.hset(data_key, {
        "ID": id,
        "QR_URL": qr_url,
        "PDF_URL": pdf_url,
        "QR_POSITION_X": qr_position_x,
        "QR_POSITION_Y": qr_position_y,
        "STATUS": "SUCCESS",
        "FLAG": "N",
        "API_CALLBACK": api_callback,
        "PATH_SOURCE": f"{pdf_source}{created_at.isoformat()}_{id}.pdf",
        "PATH_STEMPED": "",
        "STATUS_STEMPED": "",
        "CREATED_AT": created_at.isoformat(),
        "UPDATED_AT": "",
    })


@app.route('/collect_data', methods=['POST'])
def collect_data():
    try:
        data = request.json
        if not data or "DATA" not in data:
            return jsonify({"OUT_STAT": "ERROR", "MESSAGE": "Invalid request format"}), 400

        api_callback = data.get("API_CALLBACK")
        data_content = data["DATA"]

        required_fields = ["ID", "QR_URL", "PDF_URL",
                           "QR_POSITION_X", "QR_POSITION_Y"]
        if not all(field in data_content for field in required_fields):
            return jsonify({"OUT_STAT": "ERROR", "MESSAGE": "Missing required fields"}), 400

        download_file(data_content["ID"], data_content["PDF_URL"])
        insert_to_redis(data_content["ID"], data_content["QR_URL"], data_content["PDF_URL"],
                        data_content["QR_POSITION_X"], data_content["QR_POSITION_Y"], api_callback)

        return jsonify({
            "OUT_STAT": "SUCCESS",
            "OUT_DATA": {
                "QR_ID": qr_id
            }
        }), 200
    except Exception as e:
        return jsonify({"OUT_STAT": "ERROR", "MESSAGE": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
