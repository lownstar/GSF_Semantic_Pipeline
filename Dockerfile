FROM python:3.11-slim
WORKDIR /app
COPY requirements-app.txt .
RUN pip install --no-cache-dir -r requirements-app.txt
COPY . .
EXPOSE 8501
CMD streamlit run app/streamlit_app.py --server.port $PORT --server.address 0.0.0.0
