# Binance EL Pipeline
<p>In this project, an EL Pipeline (Extract-Load) was designed using the Binance API and Cloud Storage, the storage unit of Google Cloud.</p>
<p>By using the Binance API in the extract.py script, the 12-hour data of the BTCUSD coin is extracted as JSON.</p>
<p>First, a storage bucket was created using the Google Cloud Shell terminal. Required permissions have been granted to upload files via IAM. With the load.py script, the JSON data extracted from the Binance API is loaded into the created bucket.</p>
