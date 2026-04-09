from flask import Flask, render_template_string, request
import requests
import time
import re

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
        .search-row {
            display: flex;
            align-items: center;
            flex-wrap: wrap;
            gap: 10px;
        }
        .filter-row {
            margin-top: 10px;
        }
        input[type="text"] {
            padding: 10px;
            width: 400px;
            font-size: 16px;
        }
        .sort-controls {
            display: inline-block;
            margin-left: 10px;
        }
        .sort-label {
            margin-right: 4px;
            font-size: 14px;
            color: #555;
        }
        .sort-button { 
            padding: 10px 15px;
            font-size: 14px; 
            background-color: #E1E3E3;
            color: #2c3e50;
            border: 1px solid #bdc3c7;
            border-radius: 4px;
            cursor: pointer;
            margin-right: 4px;
        }
        .sort-button.active {
            background-color: #3498db;
            color: #fff;
            border-color: #2980b9;
        }
        .sort-button:hover {
            background-color: #E1E3E3;
            color: #2c3e50;
            border-color: #bdc3c7;
        }
        .sort-dropdown {
            position: relative;
            display: inline-block;
        }
        .sort-caret {
            width: 8px;
            height: 8px;
            border-right: 1.5px solid #888;
            border-bottom: 1.5px solid #888;
            transform: rotate(45deg);
            transition: transform 0.2s ease, border-color 0.2s ease;
            vertical-align: middle;
            position: relative;
            display: inline-flex;
            justify-content: space-between;
            margin-left: 6px;
            margin-bottom: 4px;
        }
        .sort-button.active .sort-caret {
            transform: rotate(-135deg);
            margin-top: 4px;
        }
        .sort-menu {
            position: absolute;
            top: 110%;
            left: 0;
            background: #fff;
            border: 1px solid #bdc3c7;
            border-radius: 4px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.15);
            padding: 8px;
            z-index: 10;
            min-width: 140px;
        }
        .sort-menu.hidden {
            display: none;
        }
        .sort-menu-title {
            font-size: 12px;
            text-transform: uppercase;
            color: #888;
            margin-bottom: 4px;
        }
        .sort-option {
            display: block;
            width: 100%;
            text-align: left;
            background: transparent;
            border: none;
            padding: 4px 2px;
            cursor: pointer;
            font-size: 14px;
            color: #000;
        }
        .sort-option.active {
            font-weight: bold;
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
            position: relative;
        }
        .result-text {
            position: relative;
            display: block;
            line-height: 1.5em;
            max-height: 3em; /* ~2 lines */
            overflow: hidden;
        }
        .result-text.clamped::after {
            content: '...';
            position: absolute;
            right: 0;
            bottom: 0;
            padding-left: 4px;
            background: #fff;
        }
        .result-text.expanded {
            max-height: none;
            overflow: visible;
        }
        .result-text.expanded::after {
            content: '';
        }
        .highlight {
            color: #2D88E3;
            font-weight: bold;
        }
        .subreddit-badge {
            display: inline-block;
            background-color: #FCDFC2;
            color: #000;
            font-size: 12px;
            padding: 2px 6px;
            border-radius: 4px;
            margin-bottom: 4px;
            word-wrap: break-word;
            font-weight: bold;
        }
        .timestamp {
            font-size: 12px;
            color: #888;
            margin-bottom: 8px;
        }
        .result-header {
            position: absolute;
            top: 10px;
            right: 10px;
            display: flex;
            flex-direction: column;
            align-items: flex-end;
            gap: 6px;
        }
        .sentiment {
            font-size: 12px;
            font-weight: bold;
            padding: 4px 10px;
            border-radius: 4px;
            color: #ffffff;
        }
        .sentiment-positive {
            background-color: #27ae60;
        }
        .sentiment-negative {
            background-color: #c0392b;
        }
        .sentiment-neutral {
            background-color: #7f8c8d;
        }
        .link-button {
            padding: 5px 10px;
            font-size: 12px;
            background-color: transparent;
            color: #555;
            border: 1px solid #bdc3c7;
            border-radius: 15px;
            cursor: pointer;
            transition: background-color 0.2s;
        }
        .link-button:hover {
            background-color: #ecf0f1;
        }
        .pagination {
            margin-top: 20px;
        }
        .pagination a {
            margin: 5px;
            text-decoration: none;
            color: #3498db;
        }
        .sentiment-filters {
            display: inline-flex;
            align-items: center;
            flex-wrap: wrap;
            gap: 6px;
        }
        .sentiment-chip {
            display: inline-flex;
            align-items: center;
            padding: 6px 10px;
            font-size: 13px;
            color: #2c3e50;
            background-color: #ffffff;
            border-radius: 999px;
            border: 1px solid #d0d7de;
            cursor: pointer;
            transition: background-color 0.15s ease, color 0.15s ease, border-color 0.15s ease;
        }
        .sentiment-chip input[type="checkbox"] {
            margin-right: 6px;
        }
        .sentiment-chip input[type="checkbox"]:checked + span {
            font-weight: 600;
        }
    </style>
</head>
<body>

<h1>🌍 Climate Search Engine</h1>

<form method="get" class="search-box">
    <div class="search-row">
        <input type="text" name="q" value="{{ query }}" placeholder="Search climate topics..." required>
        <input type="hidden" id="sort-field-input" name="sort_field" value="{{ sort_field }}">
        <button type="submit">Search</button>
        <div class="sort-controls">
            <div class="sort-dropdown">
                <button
                    type="button"
                    id="sort-display"
                    class="sort-button"
                >
                    <span id="sort-display-text">{{ 'Best' if sort_field == 'best' else 'New' }}</span>
                    <span class="sort-caret"></span>
                </button>
                <div id="sort-menu" class="sort-menu hidden">
                    <div class="sort-menu-title">Sort By</div>
                    <button
                        type="button"
                        class="sort-option {% if sort_field == 'best' %}active{% endif %}"
                        data-value="best"
                    >Best</button>
                    <button
                        type="button"
                        class="sort-option {% if sort_field == 'new' %}active{% endif %}"
                        data-value="new"
                    >New</button>
                </div>
            </div>
        </div>
    </div>
    <div class="filter-row">
        <div class="sentiment-filters">
            <span class="sort-label">Sentiment:</span>
            <label class="sentiment-chip">
                <input type="checkbox" name="sentiment" value="positive" {% if 'positive' in selected_sentiments %}checked{% endif %}>
                <span>Positive</span>
            </label>
            <label class="sentiment-chip">
                <input type="checkbox" name="sentiment" value="negative" {% if 'negative' in selected_sentiments %}checked{% endif %}>
                <span>Negative</span>
            </label>
            <label class="sentiment-chip">
                <input type="checkbox" name="sentiment" value="neutral" {% if 'neutral' in selected_sentiments %}checked{% endif %}>
                <span>Neutral</span>
            </label>
        </div>
    </div>
</form>

{% if results is not none %}

    {% if spell_suggestion and spell_suggestion != query %}
        <div class="meta">
            Did you mean:
            <a href="?q={{ spell_suggestion }}&sort_field={{ sort_field }}{% for s in selected_sentiments %}&sentiment={{ s }}{% endfor %}">{{ spell_suggestion }}</a> ?
        </div>
    {% endif %}

    <div class="meta">
        Found {{ num_found }} results in {{ elapsed }} seconds
    </div>

    {% for doc in results %}
        <div class="result">

            <div class="result-header">
                {% if doc.get('sentiment') %}
                    {% set s = doc.get('sentiment')|lower %}
                    <div class="sentiment 
                        {% if 'pos' in s %}sentiment-positive
                        {% elif 'neg' in s %}sentiment-negative
                        {% else %}sentiment-neutral
                        {% endif %}">
                        Sentiment: {% if 'pos' in s %}Positive{% elif 'neg' in s %}Negative{% else %}Neutral{% endif %}
                    </div>
                {% endif %}

                {% if doc.get('url') %}
                    <a href="{{ doc['url'] }}" target="_blank">
                        <button class="link-button">🔗 Link to Comment</button>
                    </a>
                {% endif %}
            </div>

            {% if doc.get('subreddit') %}
                <div class="subreddit-badge">
                    Subreddit: {{ doc.get('subreddit') }}
                </div>
            {% endif %}

            {% if doc.get('timestamp') %}
                <div class="timestamp">
                    Posted on: {{ doc.get('timestamp') }}
                </div>
            {% endif %}

            <p>
                <span class="result-text" id="text-{{ loop.index0 }}">
                    {{ doc.get('highlighted_full', doc.get('text', 'No content')) | safe }}
                </span>
            </p>

            <button
                type="button"
                class="link-button view-toggle"
                data-id="{{ loop.index0 }}"
            >View full text</button>

        </div>
    {% endfor %}

    {% if not results %}
        <div>No results found.</div>
    {% endif %}

    <div class="pagination">
        {% if page > 1 %}
            <a href="?q={{ query }}&page={{ page-1 }}&sort_field={{ sort_field }}{% for s in selected_sentiments %}&sentiment={{ s }}{% endfor %}">⬅ Previous</a>
        {% endif %}
        <a href="?q={{ query }}&page={{ page+1 }}&sort_field={{ sort_field }}{% for s in selected_sentiments %}&sentiment={{ s }}{% endfor %}">Next ➡</a>
    </div>

{% endif %}

<script>
document.addEventListener('DOMContentLoaded', function () {
    var displayBtn = document.getElementById('sort-display');
    var displayText = document.getElementById('sort-display-text');
    var menu = document.getElementById('sort-menu');
    var sortInput = document.getElementById('sort-field-input');
    if (!displayBtn || !displayText || !menu || !sortInput) return;

    displayBtn.addEventListener('click', function (event) {
        event.preventDefault();
        menu.classList.toggle('hidden');
        displayBtn.classList.toggle('active');
    });

    menu.addEventListener('click', function (event) {
        if (event.target.classList.contains('sort-option')) {
            var value = event.target.getAttribute('data-value');
            sortInput.value = value;
            displayText.textContent = value === 'best' ? 'Best' : 'New';

            var options = menu.querySelectorAll('.sort-option');
            options.forEach(function (opt) {
                opt.classList.remove('active');
            });
            event.target.classList.add('active');

            menu.classList.add('hidden');
        }
    });

    document.addEventListener('click', function (event) {
        if (!menu.contains(event.target) && !displayBtn.contains(event.target)) {
            menu.classList.add('hidden');
            displayBtn.classList.remove('active');
        }
    });

    // Toggle full text for each result (expand beyond 2 lines)
    var toggles = document.querySelectorAll('.view-toggle');
    toggles.forEach(function (btn) {
        btn.addEventListener('click', function () {
            var id = btn.getAttribute('data-id');
            var textEl = document.getElementById('text-' + id);
            if (!textEl) return;

            var isExpanded = textEl.classList.contains('expanded');
            if (isExpanded) {
                textEl.classList.remove('expanded');
                btn.textContent = 'View full text';
            } else {
                textEl.classList.add('expanded');
                btn.textContent = 'Hide full text';
            }
        });
    });
});
</script>

</body>
</html>
'''

# ===== Helper: highlight query words in text =====
def highlight_text(text, query):
    if not text or not query:
        return text
    words = query.strip().split()
    for word in words:
        if not word:
            continue
        # replace all occurrences of the word (case-insensitive) with highlighted span
        pattern = re.compile(r'\b' + re.escape(word) + r'\b', re.IGNORECASE)
        text = pattern.sub(r'<span class="highlight">\g<0></span>', text)
    return text

# ===== ROUTE =====
@app.route('/', methods=['GET'])
def search():
    query = request.args.get('q', '')
    page = int(request.args.get('page', 1))
    category = request.args.get('category', '')
    sort_field = request.args.get('sort_field', 'best')
    selected_sentiments = request.args.getlist('sentiment')

    rows = 10
    start = (page - 1) * rows

    spell_suggestion = None
    results = []
    num_found = 0
    elapsed = 0

    fq = []
    if category:
        fq.append(f"category:{category}")

    # Sentiment filter (multi-select)
    if selected_sentiments:
        # Support both full words and shorthand variants in Solr
        sentiment_terms = set()
        for s in selected_sentiments:
            sl = s.lower()
            if sl == 'positive':
                sentiment_terms.update(['positive', 'Positive', 'POS', 'pos'])
            elif sl == 'negative':
                sentiment_terms.update(['negative', 'Negative', 'NEG', 'neg'])
            elif sl == 'neutral':
                sentiment_terms.update(['neutral', 'Neutral', 'NEU', 'neu'])
        if sentiment_terms:
            joined = " OR ".join(sorted(sentiment_terms))
            fq.append(f"sentiment:({joined})")

    sort_param = None
    if sort_field == "new":
        sort_param = "timestamp desc"

    if query:
        params = {
            'q': query,
            'defType': 'edismax',
            'qf': 'text^2 post_title^3',
            'rows': rows,
            'start': start,
            'wt': 'json',
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

            results = data['response']['docs']
            num_found = data['response']['numFound']

            # prepare highlighted full text; truncation handled in CSS
            for doc in results:
                full_text = doc.get('text', '') or ''
                doc['highlighted_full'] = highlight_text(full_text, query)

            # SPELLCHECK
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

        elapsed = round(time.time() - t0, 4)

    return render_template_string(
        HTML,
        query=query,
        results=results,
        num_found=num_found,
        elapsed=elapsed,
        page=page,
        spell_suggestion=spell_suggestion,
        category=category,
        sort_field=sort_field,
        selected_sentiments=selected_sentiments
    )

# ===== RUN =====
if __name__ == '__main__':
    app.run(debug=True)