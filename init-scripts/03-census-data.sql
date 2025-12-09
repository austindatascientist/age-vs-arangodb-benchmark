-- Alabama major cities with 2020 US Census population data

INSERT INTO cities (name, lat, lon, population, county) VALUES
    ('Huntsville', 34.7304, -86.5861, 215006, 'Madison'),
    ('Birmingham', 33.5186, -86.8104, 200733, 'Jefferson'),
    ('Montgomery', 32.3792, -86.3077, 200603, 'Montgomery'),
    ('Mobile', 30.6954, -88.0399, 187041, 'Mobile'),
    ('Tuscaloosa', 33.2098, -87.5692, 99600, 'Tuscaloosa')
ON CONFLICT DO NOTHING;
