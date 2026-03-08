#!/usr/bin/env python3
"""Simple API server for Metro Calls for Service data."""
import json, http.server, urllib.parse, os, sys
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(BASE_DIR, 'data.csv')
JSON_FILE = os.path.join(BASE_DIR, 'data.json')
META_FILE = os.path.join(BASE_DIR, 'meta.json')

print("Loading data...")
if os.path.exists(CSV_FILE):
    import csv
    ALL_RECORDS = []
    with open(CSV_FILE, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ALL_RECORDS.append(row)
    print(f"Loaded from CSV")
else:
    with open(JSON_FILE) as f:
        ALL_RECORDS = json.load(f)
    print(f"Loaded from JSON")
with open(META_FILE) as f:
    META = json.load(f)
print(f"Loaded {len(ALL_RECORDS):,} records")

# Pre-parse dates for faster filtering
for r in ALL_RECORDS:
    try:
        r['_dt'] = datetime.strptime(r['call_time'], '%m/%d/%Y %H:%M:%S')
        r['_month'] = r['_dt'].strftime('%Y-%m')
        r['_date'] = r['_dt'].strftime('%Y-%m-%d')
        r['_hour'] = r['_dt'].hour
    except:
        r['_dt'] = None
        r['_month'] = ''
        r['_date'] = ''
        r['_hour'] = -1


class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        
        if parsed.path == '/api/meta':
            self.json_response(META)
        elif parsed.path == '/api/search':
            self.handle_search(parsed)
        elif parsed.path == '/api/stats':
            self.handle_stats(parsed)
        elif parsed.path == '/api/heatmap':
            self.handle_heatmap(parsed)
        elif parsed.path == '/api/map':
            self.handle_map(parsed)
        elif parsed.path == '/api/top-locations':
            self.handle_top_locations(parsed)
        else:
            super().do_GET()

    def handle_search(self, parsed):
        params = urllib.parse.parse_qs(parsed.query)
        filtered = self.apply_filters(params)
        
        # Pagination
        page = int(params.get('page', ['1'])[0])
        per_page = min(int(params.get('per_page', ['100'])[0]), 500)
        start = (page - 1) * per_page
        end = start + per_page
        
        results = []
        for r in filtered[start:end]:
            results.append({k: v for k, v in r.items() if not k.startswith('_')})
        
        self.json_response({
            'total': len(filtered),
            'page': page,
            'per_page': per_page,
            'pages': (len(filtered) + per_page - 1) // per_page,
            'results': results,
        })

    def handle_stats(self, parsed):
        params = urllib.parse.parse_qs(parsed.query)
        filtered = self.apply_filters(params)
        
        # Count by call type
        by_type = {}
        by_location_type = {}
        by_month = {}
        by_hour = {}
        by_disposition = {}
        by_district = {}
        
        for r in filtered:
            ct = r['call_type']
            by_type[ct] = by_type.get(ct, 0) + 1
            
            lt = r['location_type']
            by_location_type[lt] = by_location_type.get(lt, 0) + 1
            
            m = r['_month']
            by_month[m] = by_month.get(m, 0) + 1
            
            h = r['_hour']
            if h >= 0:
                by_hour[h] = by_hour.get(h, 0) + 1
            
            d = r['disposition']
            by_disposition[d] = by_disposition.get(d, 0) + 1
            
            dist = r['dist']
            by_district[dist] = by_district.get(dist, 0) + 1
        
        self.json_response({
            'total': len(filtered),
            'by_type': sorted(by_type.items(), key=lambda x: -x[1]),
            'by_location_type': sorted(by_location_type.items(), key=lambda x: -x[1]),
            'by_month': sorted(by_month.items()),
            'by_hour': sorted(by_hour.items()),
            'by_disposition': sorted(by_disposition.items(), key=lambda x: -x[1]),
            'by_district': sorted(by_district.items(), key=lambda x: -x[1]),
        })

    def handle_heatmap(self, parsed):
        params = urllib.parse.parse_qs(parsed.query)
        filtered = self.apply_filters(params)
        
        # Day of week x hour heatmap
        heatmap = {}
        for r in filtered:
            if r['_dt']:
                dow = r['_dt'].strftime('%A')
                hour = r['_hour']
                key = f"{dow}|{hour}"
                heatmap[key] = heatmap.get(key, 0) + 1
        
        self.json_response({'heatmap': heatmap, 'total': len(filtered)})

    def handle_map(self, parsed):
        params = urllib.parse.parse_qs(parsed.query)
        filtered = self.apply_filters(params)
        
        geocache_file = os.path.join(BASE_DIR, 'geocache.json')
        geocache = {}
        if os.path.exists(geocache_file):
            with open(geocache_file) as f:
                geocache = json.load(f)
        
        # Aggregate calls by location, attach geo coords
        by_location = {}
        for r in filtered:
            loc = r['location'].strip()
            if not loc:
                continue
            if loc not in by_location:
                by_location[loc] = {'count': 0, 'types': {}}
            by_location[loc]['count'] += 1
            ct = r['call_type']
            by_location[loc]['types'][ct] = by_location[loc]['types'].get(ct, 0) + 1
        
        markers = []
        for loc, info in sorted(by_location.items(), key=lambda x: -x[1]['count']):
            geo = geocache.get(loc)
            if geo and geo is not None:
                top_types = sorted(info['types'].items(), key=lambda x: -x[1])[:5]
                markers.append({
                    'lat': geo['lat'],
                    'lon': geo['lon'],
                    'location': loc,
                    'count': info['count'],
                    'top_types': top_types,
                })
        
        self.json_response({'markers': markers, 'total_mapped': sum(m['count'] for m in markers), 'total_filtered': len(filtered)})

    def handle_top_locations(self, parsed):
        params = urllib.parse.parse_qs(parsed.query)
        filtered = self.apply_filters(params)
        
        by_location = {}
        for r in filtered:
            loc = r['location'].strip()
            if not loc:
                continue
            if loc not in by_location:
                by_location[loc] = {'count': 0, 'types': {}, 'location_type': r['location_type']}
            by_location[loc]['count'] += 1
            ct = r['call_type']
            by_location[loc]['types'][ct] = by_location[loc]['types'].get(ct, 0) + 1
        
        top = sorted(by_location.items(), key=lambda x: -x[1]['count'])[:50]
        results = []
        for loc, info in top:
            top_types = sorted(info['types'].items(), key=lambda x: -x[1])[:3]
            results.append({
                'location': loc,
                'count': info['count'],
                'location_type': info['location_type'],
                'top_types': top_types,
            })
        
        self.json_response({'locations': results})

    def apply_filters(self, params):
        filtered = ALL_RECORDS
        
        q = params.get('q', [''])[0].lower()
        if q:
            filtered = [r for r in filtered if q in r['location'].lower() or q in r['call_type'].lower() or q in r['police_num'].lower()]
        
        call_type = params.get('call_type', [''])[0]
        if call_type:
            filtered = [r for r in filtered if r['call_type'] == call_type]
        
        location_type = params.get('location_type', [''])[0]
        if location_type:
            filtered = [r for r in filtered if r['location_type'] == location_type]
        
        disposition = params.get('disposition', [''])[0]
        if disposition:
            filtered = [r for r in filtered if r['disposition'] == disposition]
        
        district = params.get('district', [''])[0]
        if district:
            filtered = [r for r in filtered if r['dist'] == district]
        
        date_from = params.get('date_from', [''])[0]
        if date_from:
            filtered = [r for r in filtered if r['_date'] >= date_from]
        
        date_to = params.get('date_to', [''])[0]
        if date_to:
            filtered = [r for r in filtered if r['_date'] <= date_to]
        
        month = params.get('month', [''])[0]
        if month:
            filtered = [r for r in filtered if r['_month'] == month]
        
        return filtered

    def json_response(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(body))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)


if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
    server = http.server.HTTPServer(('0.0.0.0', port), Handler)
    print(f"Metro CFS Explorer running at http://localhost:{port}")
    server.serve_forever()
