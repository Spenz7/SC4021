from flask import Flask, render_template_string, request
import requests
import time

app = Flask(__name__)

# ===== SOLR CONFIG =====
SOLR_URL = 'http://localhost:8983/solr/climate_change_core/select'

# ===== HTML TEMPLATE =====
HTML = '''
<!DOCTYPE html>
<html>
<head>
    <title>Climate Search Engine</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 40px;
            background-color: #f5f5f5;
        }
        h1 {
            color: #2c3e50;
        }
        .search-box {
            margin-bottom: 20px;
        }
        input[type="text"] {
            padding: 10px;
            width: 400px;
            font-size: 16px;
        }
        button {
            padding: 10px 15px;
            font-size: 16px;
            background-color: #3498db;
            color: white;
            border: none;
            cursor: pointer;
        }
        button:hover {
            background-color: #2980b9;
        }
        .meta {
            margin-top: 10px;
            margin-bottom: 20px;
            color: #555;
        }
        .result {
            background: white;
            padding: 15px;
            margin-bottom: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .result h3 {
            margin: 0;
            color: #2c3e50;
        }
        .result p {
            margin: 10px 0;
        }
        .highlight {
            background-color: yellow;
        }
        .pagination {
            margin-top: 20px;
        }
        .pagination a {
            margin: 5px;
            text-decoration: none;
            color: #3498db;
        }
    </style>
</head>
<body>

<h1>🌍 Climate Search Engine</h1>

<form method="get" class="search-box">
    <input type="text" name="q" value="{{ query }}" placeholder="Search climate topics..." required>
    <button type="submit">Search</button>
</form>

{% if results is not none %}

    <!-- ✅ DID YOU MEAN -->
    {% if spell_suggestion and spell_suggestion != query %}
        <div class="meta">
            Did you mean:
            <a href="?q={{ spell_suggestion }}">{{ spell_suggestion }}</a> ?
        </div>
    {% endif %}

    <!-- ✅ META INFO -->
    <div class="meta">
        Found {{ num_found }} results in {{ elapsed }} seconds
    </div>

    <!-- ✅ RESULTS -->
    {% for doc in results %}
        <div class="result">
            <p>
                {% if highlighting.get(doc['id']) %}
                    {{ highlighting[doc['id']]['text'][0] | safe }}
                {% else %}
                    {{ doc.get('text', 'No content')[:300] }}...
                {% endif %}
            </p>

            {% if doc.get('url') %}
                <a href="{{ doc['url'] }}" target="_blank">
                    <button>🔗 Link to Comment</button>
                </a>
            {% endif %}

            {% if doc.get('subreddit') %}
                <div class="meta">
                    Subreddit: {{ doc.get('subreddit') }}
                </div>
            {% endif %}
        </div>
    {% endfor %}

    {% if not results %}
        <div>No results found.</div>
    {% endif %}

    <!-- ✅ PAGINATION -->
    <div class="pagination">
        {% if page > 1 %}
            <a href="?q={{ query }}&page={{ page-1 }}">⬅ Previous</a>
        {% endif %}

        <a href="?q={{ query }}&page={{ page+1 }}">Next ➡</a>
    </div>

{% endif %}

</body>
</html>
'''

# ===== ROUTE =====
@app.route('/', methods=['GET'])
def search():
    query = request.args.get('q', '')
    page = int(request.args.get('page', 1))
    category = request.args.get('category', '')
    sort = request.args.get('sort', 'relevance')

    rows = 10
    start = (page - 1) * rows

    # ===== INIT VARIABLES =====
    spell_suggestion = None
    results = []
    num_found = 0
    highlighting = {}
    elapsed = 0  # initialize to 0 so template won't crash

    # ===== FILTER QUERY =====
    fq = []
    if category:
        fq.append(f"category:{category}")

    # ===== SORTING =====
    sort_param = None
    if sort == "date":
        sort_param = "date desc"

    if query:
        params = {
            'q': query,
            'defType': 'edismax',
            'qf': 'text^2 post_title^3',
            'rows': rows,
            'start': start,
            'wt': 'json',
            'hl': 'true',
            'hl.fl': 'text',
            'hl.simple.pre': '<span class="highlight">',
            'hl.simple.post': '</span>',
            'spellcheck': 'true',
            'spellcheck.q': query,
            'spellcheck.collate': 'true',
        }

        if fq:
            params['fq'] = fq

        if sort_param:
            params['sort'] = sort_param

        t0 = time.time()

        try:
            r = requests.get(SOLR_URL, params=params)
            data = r.json()

            # ===== RESULTS =====
            results = data['response']['docs']
            num_found = data['response']['numFound']
            highlighting = data.get('highlighting', {})

            # ===== SPELLCHECK =====
            spellcheck_data = data.get('spellcheck', {})
            collations = spellcheck_data.get('collations', [])

            if collations:
                for i in range(len(collations)-1, -1, -1):
                    item = collations[i]
                    if isinstance(item, str) and item.lower() != 'collation':
                        spell_suggestion = item
                        break

        except Exception as e:
            print("Error:", e)

        elapsed = round(time.time() - t0, 4)  # always set elapsed

    return render_template_string(
        HTML,
        query=query,
        results=results,
        num_found=num_found,
        elapsed=elapsed,
        highlighting=highlighting,
        page=page,
        spell_suggestion=spell_suggestion,
        category=category,
        sort=sort
    )

# ===== RUN =====
if __name__ == '__main__':
    app.run(debug=True)