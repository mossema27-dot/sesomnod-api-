#!/bin/bash
API_KEY="a9a1f6239e6431ec9183ed2582250927"

# 1. Torino vs Parma (Serie A)
echo "=== TORINO VS PARMA ==="
curl -s "https://api.the-odds-api.com/v4/sports/soccer_italy_serie_a/odds?apiKey=${API_KEY}&regions=eu&markets=h2h" | \
jq '.[] | select(.home_team=="Torino FC" and .away_team=="Parma") | {match: "\(.home_team) vs \(.away_team)", time: .commence_time, bookmakers: [.bookmakers[] | select(.title=="Pinnacle" or .title=="Unibet") | {name: .title, odds: .markets[0].outcomes}]}'

# 2. Atlético vs Getafe (La Liga)
echo "=== ATLETICO VS GETAFE ==="
curl -s "https://api.the-odds-api.com/v4/sports/soccer_spain_la_liga/odds?apiKey=${API_KEY}&regions=eu&markets=h2h" | \
jq '.[] | select(.home_team=="Atletico Madrid" and .away_team=="Getafe") | {match: "\(.home_team) vs \(.away_team)", time: .commence_time, bookmakers: [.bookmakers[] | select(.title=="Pinnacle" or .title=="Unibet") | {name: .title, odds: .markets[0].outcomes}]}'

# 3. Go Ahead Eagles vs NAC Breda (Eredivisie)
echo "=== GO AHEAD EAGLES VS NAC BREDA ==="
curl -s "https://api.the-odds-api.com/v4/sports/soccer_netherlands_eredivisie/odds?apiKey=${API_KEY}&regions=eu&markets=h2h" | \
jq '.[] | select(.home_team=="Go Ahead Eagles" and .away_team=="NAC Breda") | {match: "\(.home_team) vs \(.away_team)", time: .commence_time, bookmakers: [.bookmakers[] | select(.title=="Pinnacle" or .title=="Unibet") | {name: .title, odds: .markets[0].outcomes}]}'

# 4. FC Twente vs FC Utrecht (Eredivisie)
echo "=== FC TWENTE VS FC UTRECHT ==="
curl -s "https://api.the-odds-api.com/v4/sports/soccer_netherlands_eredivisie/odds?apiKey=${API_KEY}&regions=eu&markets=h2h" | \
jq '.[] | select(.home_team=="FC Twente" and .away_team=="FC Utrecht") | {match: "\(.home_team) vs \(.away_team)", time: .commence_time, bookmakers: [.bookmakers[] | select(.title=="Pinnacle" or .title=="Unibet") | {name: .title, odds: .markets[0].outcomes}]}'