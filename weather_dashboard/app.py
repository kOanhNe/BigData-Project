from flask import Flask, jsonify, request, send_file
from pyhive import hive
import pandas as pd
import io
import math

from visualizations import (
    create_temperature_trends_plot,
    create_distributions_plot,
    create_temp_humidity_scatter_plot,
    create_precip_temp_plot,
    create_humidity_precip_plot,
    create_correlation_heatmap
)

app = Flask(__name__)

HIVE_HOST = 'localhost'
HIVE_PORT = 10000
HIVE_USERNAME = 'hive'
DATABASE_NAME = 'default'
TABLE_NAME = 'weather_data'

try:
    weather_hourly_csv = pd.read_csv("weather_cleaned.csv", parse_dates=["Datetime"])
    weather_daily_csv = weather_hourly_csv.set_index("Datetime").resample("D").agg({
        "Temperature": ["mean", "min", "max"],
        "Humidity": "mean",
        "Precipitation": "sum",
        "WindSpeed": "mean",
        "Pressure": "mean",
        "CloudCover": "mean"
    })
    weather_daily_csv.columns = ["_".join(col).strip() for col in weather_daily_csv.columns.values]
    weather_daily_csv.rename(columns={
        'Temperature_mean': 'Temp_mean', 'Temperature_min': 'Temp_min',
        'Temperature_max': 'Temp_max', 'Humidity_mean': 'Humidity',
        'Precipitation_sum': 'Precip', 'WindSpeed_mean': 'WindSpeed',
        'Pressure_mean': 'Pressure', 'CloudCover_mean': 'Cloud'
    }, inplace=True)
except FileNotFoundError:
    print("Cảnh báo: File 'weather_cleaned.csv' không tìm thấy. Các biểu đồ tĩnh sẽ không hoạt động.")
    weather_hourly_csv, weather_daily_csv = None, None

HTML_CONTENT = """
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard & Data Explorer</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Inter', sans-serif; background-color: #f8fafc; }
        .card { background-color: white; border-radius: 0.75rem; box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1); }
        .chart-card { padding: 1rem; display: flex; justify-content: center; align-items: center; min-height: 400px; }
        .chart-card img { max-width: 100%; height: auto; }
        .loader { border: 4px solid #e5e7eb; border-radius: 50%; border-top: 4px solid #3b82f6; width: 40px; height: 40px; animation: spin 1.5s linear infinite; }
        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
        .pagination-button { background-color: #e5e7eb; color: #374151; font-semibold; padding: 0.5rem 1rem; border-radius: 0.375rem; transition: background-color 0.2s; border: 1px solid transparent; }
        .pagination-button:hover:not(:disabled) { background-color: #d1d5db; }
        .pagination-button:disabled { background-color: #f3f4f6; color: #9ca3af; cursor: not-allowed; }
    </style>
</head>
<body class="p-4 md:p-6">
    <div class="max-w-screen-2xl mx-auto">
        <header class="mb-6 text-center"><h1 class="text-4xl font-bold text-gray-900">Dashboard Thời tiết Tổng hợp</h1></header>
        <div class="grid grid-cols-1 xl:grid-cols-2 gap-6">
            <section id="static-dashboard">
                <div class="card p-4">
                    <h2 class="text-2xl font-semibold text-gray-800 mb-4 text-center">Báo cáo Phân tích (từ file CSV)</h2>
                    <div class="mb-4">
                        <label for="chart-selector" class="block text-sm font-medium text-gray-700 mb-1">Chọn biểu đồ để xem:</label>
                        <select id="chart-selector" onchange="updateChart()" class="w-full p-2 border border-gray-300 rounded-md bg-white focus:ring-blue-500 focus:border-blue-500">
                            <option value="/plot/temperature_trends.png" selected>1. Biến động nhiệt độ</option>
                            <option value="/plot/distributions.png">2. Phân bố Nhiệt độ & Độ ẩm</option>
                            <option value="/plot/correlation_heatmap.png">3. Ma trận tương quan</option>
                            <option value="/plot/temp_humidity_scatter.png">4. Tương quan Nhiệt độ & Độ ẩm</option>
                            <option value="/plot/precip_temp.png">5. Tương quan Mưa & Nhiệt độ</option>
                            <option value="/plot/humidity_precip.png">6. Tương quan Độ ẩm & Mưa</option>
                        </select>
                    </div>
                    <div class="chart-card border border-gray-200"><img id="chart-image" src="/plot/temperature_trends.png" alt="Biểu đồ được chọn"></div>
                </div>
            </section>
            <section id="hive-explorer">
                <div class="card p-4 flex flex-col h-full">
                     <h2 class="text-2xl font-semibold text-gray-800 mb-4 text-center">Khám phá Dữ liệu (trực tiếp từ Hive)</h2>
                     <div class="flex items-center space-x-2 mb-4">
                         <input type="text" id="search-input" placeholder="Tìm kiếm theo ngày, nhiệt độ,..." class="w-full p-2 border border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500">
                         <button onclick="searchData()" class="bg-blue-600 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded-md whitespace-nowrap">Tìm kiếm</button>
                     </div>
                     <div class="overflow-y-auto flex-grow">
                         <table class="w-full text-sm text-left text-gray-600">
                             <thead class="text-xs text-gray-700 uppercase bg-gray-100 sticky top-0">
                                 <tr>
                                     <th scope="col" class="px-4 py-3 w-16">STT</th>
                                     <th scope="col" class="px-4 py-3">Thời gian</th>
                                     <th scope="col" class="px-4 py-3">Nhiệt độ</th>
                                     <th scope="col" class="px-4 py-3">Độ ẩm</th>
                                     <th scope="col" class="px-4 py-3">Mưa</th>
                                     <th scope="col" class="px-4 py-3">Gió</th>
                                 </tr>
                             </thead>
                             <tbody id="hive-table-body"></tbody>
                         </table>
                         <div id="hive-loader" class="w-full flex justify-center p-8"><div class="loader"></div></div>
                     </div>
                     <div id="pagination-controls" class="flex justify-between items-center mt-4 pt-4 border-t">
                         <button id="prev-button" onclick="changePage(-1)" class="pagination-button">Trang trước</button>
                         <span id="page-info" class="text-sm font-medium text-gray-700"></span>
                         <button id="next-button" onclick="changePage(1)" class="pagination-button">Trang sau</button>
                     </div>
                </div>
            </section>
        </div>
    </div>
    <script>
        let currentPage = 1;
        const limit = 200; // Định nghĩa limit ở đây để script có thể truy cập

        function updateChart() {
            const selector = document.getElementById('chart-selector');
            const imageElement = document.getElementById('chart-image');
            imageElement.src = selector.value;
        }

        function updatePaginationControls(totalItems, page) {
            const pageInfo = document.getElementById('page-info');
            const prevButton = document.getElementById('prev-button');
            const nextButton = document.getElementById('next-button');

            if (totalItems <= 0) {
                pageInfo.textContent = 'Không có dữ liệu';
                prevButton.disabled = true;
                nextButton.disabled = true;
                return;
            }
            const totalPages = Math.ceil(totalItems / limit);
            pageInfo.textContent = `Trang ${page} / ${totalPages}`;
            prevButton.disabled = page <= 1;
            nextButton.disabled = page >= totalPages;
        }

        function renderHiveTable(data) {
            const tableBody = document.getElementById('hive-table-body');
            tableBody.innerHTML = '';
            if (!data || data.length === 0) {
                tableBody.innerHTML = '<tr><td colspan="6" class="text-center p-4">Không tìm thấy dữ liệu.</td></tr>';
                return;
            }
            data.forEach(row => {
                const tr = `
                    <tr class="bg-white border-b hover:bg-gray-50">
                        <td class="px-4 py-2 font-medium text-gray-900">${row.row_num || ''}</td>
                        <td class="px-4 py-2">${row.event_time || ''}</td>
                        <td class="px-4 py-2">${row.temperature != null ? row.temperature + '°C' : ''}</td>
                        <td class="px-4 py-2">${row.humidity != null ? row.humidity + '%' : ''}</td>
                        <td class="px-4 py-2">${row.precipitation != null ? row.precipitation + ' mm' : ''}</td>
                        <td class="px-4 py-2">${row.wind_speed != null ? row.wind_speed + ' km/h' : ''}</td>
                    </tr>`;
                tableBody.innerHTML += tr;
            });
        }

        async function fetchHiveData(page = 1) {
            const searchInput = document.getElementById('search-input').value;
            const loader = document.getElementById('hive-loader');
            const tableBody = document.getElementById('hive-table-body');

            loader.style.display = 'flex';
            tableBody.innerHTML = '';
            document.getElementById('pagination-controls').style.visibility = 'hidden';

            let url = `/api/hive-data?page=${page}&limit=${limit}`;
            if (searchInput) {
                url += '&search=' + encodeURIComponent(searchInput);
            }

            try {
                const response = await fetch(url);
                const result = await response.json();
                if (response.ok) {
                    currentPage = result.page;
                    renderHiveTable(result.data);
                    updatePaginationControls(result.total, result.page);
                } else {
                    throw new Error(result.error || 'Lỗi không xác định.');
                }
            } catch (error) {
                console.error("Lỗi khi tải dữ liệu từ Hive:", error);
                tableBody.innerHTML = `<tr><td colspan="6" class="text-center p-4 text-red-500">Lỗi: ${error.message}</td></tr>`;
                updatePaginationControls(0, 1);
            } finally {
                loader.style.display = 'none';
                document.getElementById('pagination-controls').style.visibility = 'visible';
            }
        }

        function changePage(direction) {
            const newPage = currentPage + direction;
            if (newPage > 0) {
                fetchHiveData(newPage);
            }
        }

        function searchData() {
            fetchHiveData(1);
        }

        document.addEventListener('DOMContentLoaded', () => fetchHiveData(currentPage));
    </script>
</body>
</html>
"""

@app.route('/')
def home():
    return HTML_CONTENT

@app.route('/plot/temperature_trends.png')
def plot_temp_trends():
    if weather_daily_csv is not None:
        img_buffer = create_temperature_trends_plot(weather_daily_csv)
        return send_file(img_buffer, mimetype='image/png')
    return "Dữ liệu CSV chưa được tải", 404

@app.route('/plot/distributions.png')
def plot_distributions():
    if weather_hourly_csv is not None:
        img_buffer = create_distributions_plot(weather_hourly_csv)
        return send_file(img_buffer, mimetype='image/png')
    return "Dữ liệu CSV chưa được tải", 404

@app.route('/plot/correlation_heatmap.png')
def plot_correlation_heatmap():
    if weather_hourly_csv is not None:
        img_buffer = create_correlation_heatmap(weather_hourly_csv)
        return send_file(img_buffer, mimetype='image/png')
    return "Dữ liệu CSV chưa được tải", 404

@app.route('/plot/temp_humidity_scatter.png')
def plot_temp_humidity_scatter():
    if weather_hourly_csv is not None:
        img_buffer = create_temp_humidity_scatter_plot(weather_hourly_csv)
        return send_file(img_buffer, mimetype='image/png')
    return "Dữ liệu CSV chưa được tải", 404

@app.route('/plot/precip_temp.png')
def plot_precip_temp():
    if weather_daily_csv is not None:
        img_buffer = create_precip_temp_plot(weather_daily_csv)
        return send_file(img_buffer, mimetype='image/png')
    return "Dữ liệu CSV chưa được tải", 404

@app.route('/plot/humidity_precip.png')
def plot_humidity_precip():
    if weather_daily_csv is not None:
        img_buffer = create_humidity_precip_plot(weather_daily_csv)
        return send_file(img_buffer, mimetype='image/png')
    return "Dữ liệu CSV chưa được tải", 404

@app.route('/api/hive-data', methods=['GET'])
def get_hive_data():
    conn, cursor = None, None
    search_term = request.args.get('search', '')

    try:
        page = int(request.args.get('page', 1))
        if page < 1: page = 1
    except (ValueError, TypeError):
        page = 1

    try:
        limit = int(request.args.get('limit', 200))
        if limit < 1: limit = 200
    except (ValueError, TypeError):
        limit = 200

    try:
        conn = hive.connect(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USERNAME, database=DATABASE_NAME)
        cursor = conn.cursor()

        where_clause = ""
        if search_term:
            search_pattern = f"%{search_term}%"
            where_clause = f"""
            WHERE CAST(Datetime AS STRING) LIKE '{search_pattern}' 
               OR CAST(Temperature AS STRING) LIKE '{search_pattern}'
               OR CAST(Humidity AS STRING) LIKE '{search_pattern}'
               OR CAST(Precipitation AS STRING) LIKE '{search_pattern}'
            """

        data_query = f"""
            SELECT Datetime AS event_time, Temperature AS temperature, Humidity AS humidity,
                   Precipitation AS precipitation, WindSpeed AS wind_speed
            FROM {TABLE_NAME} {where_clause}
        """

        cursor.execute(data_query)
        column_names = [desc[0].split('.')[-1] for desc in cursor.description]
        all_results = cursor.fetchall()

        if not all_results:
            return jsonify({"data": [], "total": 0, "page": page, "limit": limit})

        df = pd.DataFrame(all_results, columns=column_names)

        total_records = len(df)

        start_index = (page - 1) * limit
        end_index = start_index + limit

        page_df = df.iloc[start_index:end_index].copy()

        page_df['row_num'] = range(start_index + 1, start_index + len(page_df) + 1)

        results = page_df.to_dict(orient='records')

        return jsonify({
            "data": results,
            "total": total_records,
            "page": page,
            "limit": limit
        })

    except Exception as e:
        error_message = f"Không thể kết nối hoặc truy vấn Hive. Vui lòng kiểm tra lại dịch vụ. Chi tiết: {e}"
        print(f"Lỗi API: {error_message}")
        return jsonify({"error": error_message}), 500
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

if __name__ == '__main__':
    print("Mở trình duyệt và truy cập: http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
