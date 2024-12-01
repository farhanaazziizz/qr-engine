from flask import Flask, request, jsonify
import requests
import uuid
import redis
from datetime import datetime
import pytz
import os
from io import BytesIO
import qrcode
import fitz
import threading


app = Flask(__name__)

REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')
REDIS_DB = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True
)

pdf_source = './pdf_source/'
pdf_stemped = './pdf_stemped/'
created_at = str(datetime.now(pytz.timezone(
    'Asia/Jakarta')).strftime('%Y%m%d%H%M%S'))
qr_id = str(uuid.uuid4())
DEFAULT_QR_SIZE = 100


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


def generate_qr(qr_url: str) -> BytesIO:
    qr_code = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )
    qr_code.add_data(qr_url)
    qr_code.make(fit=True)
    qr_image = qr_code.make_image()

    img_byte_array = BytesIO()
    qr_image.save(img_byte_array)
    img_byte_array.seek(0)
    return img_byte_array


def stemp_qr(pdf_path: str, qr_path: BytesIO, qr_position_x: int, qr_position_y: int, output_pdf_stemped: str):
    try:
        pdf_document = fitz.open(pdf_path)
    except Exception as e:
        raise FileNotFoundError(
            f"Failed to open PDF file: {pdf_path}. Error: {e}")

    try:
        for page_num in range(pdf_document.page_count):
            page = pdf_document[page_num]
            page.insert_image(
                fitz.Rect(
                    qr_position_x,
                    qr_position_y,
                    qr_position_x + DEFAULT_QR_SIZE,
                    qr_position_y + DEFAULT_QR_SIZE,
                ),
                stream=qr_path,
            )

        pdf_document.save(output_pdf_stemped)
    except Exception as e:
        raise RuntimeError(f"Failed to modify the PDF. Error: {e}")
    finally:
        pdf_document.close()


def insert_to_redis(id, qr_url, pdf_url, qr_position_x, qr_position_y, api_callback):
    output_path = f"{pdf_source}{created_at}_{id}.pdf"
    data_key = f"data:{qr_id}"
    REDIS_DB.hmset(data_key, {
        "ID": id,
        "QR_URL": qr_url,
        "PDF_URL": pdf_url,
        "QR_POSITION_X": qr_position_x,
        "QR_POSITION_Y": qr_position_y,
        "STATUS": "SUCCESS",
        "FLAG": "N",
        "API_CALLBACK": api_callback,
        "PATH_SOURCE": output_path,
        "PATH_STEMPED": "",
        "STATUS_STEMPED": "",
        "CREATED_AT": created_at,
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


@app.route('/stemp_pdf', methods=['POST'])
def stemp():
    try:
        updated_at = str(datetime.now(pytz.timezone(
            'Asia/Jakarta')).strftime('%Y%m%d%H%M%S'))
        data = request.json
        data_key = data.get("DATA_KEY")

        if not data_key:
            return jsonify({"OUT_STAT": "ERROR", "MESSAGE": "DATA_KEY is required"}), 400

        redis_data = REDIS_DB.hgetall(data_key)
        if not redis_data:
            return jsonify({"OUT_STAT": "ERROR", "MESSAGE": "Data not found in Redis"}), 404

        id = redis_data.get("ID")
        qr_url = redis_data.get("QR_URL")
        path_source = redis_data.get("PATH_SOURCE")
        qr_position_x = int(redis_data.get("QR_POSITION_X"))
        qr_position_y = int(redis_data.get("QR_POSITION_Y"))
        api_callback = redis_data.get("API_CALLBACK")

        qr_path = generate_qr(qr_url)
        output_pdf_stemped = f"{pdf_stemped}{updated_at}_{id}_qr_stemped.pdf"
        stemp_qr(path_source, qr_path, qr_position_x,
                 qr_position_y, output_pdf_stemped)

        REDIS_DB.hset(data_key, "UPDATED_AT", updated_at)
        REDIS_DB.hset(data_key, "PATH_STEMPED", output_pdf_stemped)
        REDIS_DB.hset(data_key, "STATUS_STEMPED", "SUCCESS")
        REDIS_DB.hset(data_key, "FLAG", "Y")

        if api_callback and api_callback.strip():
            callback_url = f"{api_callback}?id={id}"
            callback_response = requests.get(callback_url)

        return jsonify({
            "OUT_STAT": "SUCCESS",
            "OUT_DATA": {
                "PATH_STEMPED": output_pdf_stemped
            }
        }), 200

    except Exception as e:
        return jsonify({"OUT_STAT": "ERROR", "MESSAGE": str(e)}), 500


@app.route('/get_pdf', methods=['POST'])
def get_pdf():
    try:
        data = request.json
        data_key = data.get("QR_ID")

        if not data_key:
            return jsonify({"OUT_STAT": "ERROR", "MESSAGE": "DATA_KEY is required"}), 400

        redis_data = REDIS_DB.hgetall(data_key)
        if not redis_data:
            return jsonify({"OUT_STAT": "ERROR", "MESSAGE": "Data not found in Redis"}), 404

        path_stemped = redis_data.get("PATH_STEMPED")
        if not path_stemped:
            return jsonify({"OUT_STAT": "ERROR", "MESSAGE": "Path stemped not found in Redis"}), 404

        download_url = f"http://{request.host}/download/{os.path.basename(path_stemped)}"

        return jsonify({
            "OUT_STAT": "SUCCESS",
            "OUT_DATA": {
                "PATH_STEMPED_URL": download_url
            }
        }), 200

    except Exception as e:
        return jsonify({"OUT_STAT": "ERROR", "MESSAGE": str(e)}), 500


@app.route('/download/<filename>', methods=['GET'])
def download_pdf(filename):
    try:
        return send_from_directory(pdf_stemped, filename, as_attachment=True)

    except Exception as e:
        return jsonify({"OUT_STAT": "ERROR", "MESSAGE": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
