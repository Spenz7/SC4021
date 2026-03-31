# this is just using a simple flask app that will provide a web UI and
# allow us to query a Solr index.
# to be able to run make sure to install dependencies (just do pip install -r requirements.txt)


from flask import Flask, render_template_string, request
import requests
import time


# SOLR config -- i think this is right?
SOLR_URL = 'http://localhost:8983/solr/climate_change_core/select'

app = Flask(__name__)

HTML = '''
<!DOCTYPE html>
<html>
<head>
    <title>Solr Search UI</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .search-box { margin-bottom: 20px; }
        .result { margin-bottom: 10px; padding: 10px; border: 1px solid #ddd; }
        .meta { color: #888; font-size: 0.9em; }
    </style>
</head>
<body>
    <h1>Solr Search UI</h1>
    <form method="get" class="search-box">
        <input type="text" name="q" value="{{ query }}" size="40" placeholder="please enter your search keywords" required>
        <button type="submit">Search</button>
    </form>
    {% if results is not none %}
        <div class="meta">{{ num_found }} results in {{ elapsed }}s</div>
        {% for doc in results %}
            <div class="result">
                {% for k, v in doc.items() %}
                    <b>{{ k }}:</b> {{ v }}<br>
                {% endfor %}
            </div>
        {% endfor %}
        {% if not results %}
            <div>No results found.</div>
        {% endif %}
    {% endif %}
</body>
</html>
'''

@app.route('/', methods=['GET'])
def search():
    query = request.args.get('q', '')
    results = None
    num_found = 0
    elapsed = 0
    if query:
        params = {
            'q': query,
            'rows': 10,
            'wt': 'json'
        }
        t0 = time.time()
        try:
            r = requests.get(SOLR_URL, params=params)
            r.raise_for_status()
            data = r.json()['response']
            results = data['docs']
            num_found = data['numFound']
        except Exception as e:
            results = []
            num_found = 0
        elapsed = round(time.time() - t0, 3)
    return render_template_string(HTML, query=query, results=results, num_found=num_found, elapsed=elapsed)

if __name__ == '__main__':
    app.run(debug=True)
