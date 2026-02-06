// US Airports Database - searchable by IATA, ICAO, name, or city
const US_AIRPORTS = [
    // Major Hubs
    { icao: "KATL", iata: "ATL", name: "Hartsfield-Jackson Atlanta International", city: "Atlanta", state: "GA" },
    { icao: "KBOS", iata: "BOS", name: "Logan International", city: "Boston", state: "MA" },
    { icao: "KBWI", iata: "BWI", name: "Baltimore/Washington International", city: "Baltimore", state: "MD" },
    { icao: "KCLE", iata: "CLE", name: "Cleveland Hopkins International", city: "Cleveland", state: "OH" },
    { icao: "KCLT", iata: "CLT", name: "Charlotte Douglas International", city: "Charlotte", state: "NC" },
    { icao: "KCVG", iata: "CVG", name: "Cincinnati/Northern Kentucky International", city: "Cincinnati", state: "OH" },
    { icao: "KDCA", iata: "DCA", name: "Ronald Reagan Washington National", city: "Washington", state: "DC" },
    { icao: "KDEN", iata: "DEN", name: "Denver International", city: "Denver", state: "CO" },
    { icao: "KDFW", iata: "DFW", name: "Dallas/Fort Worth International", city: "Dallas", state: "TX" },
    { icao: "KDTW", iata: "DTW", name: "Detroit Metropolitan Wayne County", city: "Detroit", state: "MI" },
    { icao: "KEWR", iata: "EWR", name: "Newark Liberty International", city: "Newark", state: "NJ" },
    { icao: "KFLL", iata: "FLL", name: "Fort Lauderdale-Hollywood International", city: "Fort Lauderdale", state: "FL" },
    { icao: "KHOU", iata: "HOU", name: "William P Hobby", city: "Houston", state: "TX" },
    { icao: "KIAD", iata: "IAD", name: "Washington Dulles International", city: "Washington", state: "DC" },
    { icao: "KIAH", iata: "IAH", name: "George Bush Intercontinental", city: "Houston", state: "TX" },
    { icao: "KJFK", iata: "JFK", name: "John F Kennedy International", city: "New York", state: "NY" },
    { icao: "KLAS", iata: "LAS", name: "Harry Reid International", city: "Las Vegas", state: "NV" },
    { icao: "KLAX", iata: "LAX", name: "Los Angeles International", city: "Los Angeles", state: "CA" },
    { icao: "KLGA", iata: "LGA", name: "LaGuardia", city: "New York", state: "NY" },
    { icao: "KMCI", iata: "MCI", name: "Kansas City International", city: "Kansas City", state: "MO" },
    { icao: "KMCO", iata: "MCO", name: "Orlando International", city: "Orlando", state: "FL" },
    { icao: "KMDW", iata: "MDW", name: "Chicago Midway International", city: "Chicago", state: "IL" },
    { icao: "KMEM", iata: "MEM", name: "Memphis International", city: "Memphis", state: "TN" },
    { icao: "KMIA", iata: "MIA", name: "Miami International", city: "Miami", state: "FL" },
    { icao: "KMKE", iata: "MKE", name: "General Mitchell International", city: "Milwaukee", state: "WI" },
    { icao: "KMSP", iata: "MSP", name: "Minneapolis-Saint Paul International", city: "Minneapolis", state: "MN" },
    { icao: "KMSY", iata: "MSY", name: "Louis Armstrong New Orleans International", city: "New Orleans", state: "LA" },
    { icao: "KOAK", iata: "OAK", name: "Oakland International", city: "Oakland", state: "CA" },
    { icao: "KONT", iata: "ONT", name: "Ontario International", city: "Ontario", state: "CA" },
    { icao: "KORD", iata: "ORD", name: "O'Hare International", city: "Chicago", state: "IL" },
    { icao: "KPBI", iata: "PBI", name: "Palm Beach International", city: "West Palm Beach", state: "FL" },
    { icao: "KPDX", iata: "PDX", name: "Portland International", city: "Portland", state: "OR" },
    { icao: "KPHL", iata: "PHL", name: "Philadelphia International", city: "Philadelphia", state: "PA" },
    { icao: "KPHX", iata: "PHX", name: "Phoenix Sky Harbor International", city: "Phoenix", state: "AZ" },
    { icao: "KPIT", iata: "PIT", name: "Pittsburgh International", city: "Pittsburgh", state: "PA" },
    { icao: "KRDU", iata: "RDU", name: "Raleigh-Durham International", city: "Raleigh", state: "NC" },
    { icao: "KRSW", iata: "RSW", name: "Southwest Florida International", city: "Fort Myers", state: "FL" },
    { icao: "KSAN", iata: "SAN", name: "San Diego International", city: "San Diego", state: "CA" },
    { icao: "KSAT", iata: "SAT", name: "San Antonio International", city: "San Antonio", state: "TX" },
    { icao: "KSDF", iata: "SDF", name: "Louisville International", city: "Louisville", state: "KY" },
    { icao: "KSEA", iata: "SEA", name: "Seattle-Tacoma International", city: "Seattle", state: "WA" },
    { icao: "KSFO", iata: "SFO", name: "San Francisco International", city: "San Francisco", state: "CA" },
    { icao: "KSJC", iata: "SJC", name: "San Jose International", city: "San Jose", state: "CA" },
    { icao: "KSLC", iata: "SLC", name: "Salt Lake City International", city: "Salt Lake City", state: "UT" },
    { icao: "KSMF", iata: "SMF", name: "Sacramento International", city: "Sacramento", state: "CA" },
    { icao: "KSNA", iata: "SNA", name: "John Wayne Airport", city: "Santa Ana", state: "CA" },
    { icao: "KSTL", iata: "STL", name: "St Louis Lambert International", city: "St Louis", state: "MO" },
    { icao: "KTPA", iata: "TPA", name: "Tampa International", city: "Tampa", state: "FL" },
    { icao: "KTUS", iata: "TUS", name: "Tucson International", city: "Tucson", state: "AZ" },

    // Secondary/Regional Airports
    { icao: "KABQ", iata: "ABQ", name: "Albuquerque International Sunport", city: "Albuquerque", state: "NM" },
    { icao: "KAUS", iata: "AUS", name: "Austin-Bergstrom International", city: "Austin", state: "TX" },
    { icao: "KBDL", iata: "BDL", name: "Bradley International", city: "Hartford", state: "CT" },
    { icao: "KBHM", iata: "BHM", name: "Birmingham-Shuttlesworth International", city: "Birmingham", state: "AL" },
    { icao: "KBNA", iata: "BNA", name: "Nashville International", city: "Nashville", state: "TN" },
    { icao: "KBOI", iata: "BOI", name: "Boise Airport", city: "Boise", state: "ID" },
    { icao: "KBUF", iata: "BUF", name: "Buffalo Niagara International", city: "Buffalo", state: "NY" },
    { icao: "KBUR", iata: "BUR", name: "Hollywood Burbank", city: "Burbank", state: "CA" },
    { icao: "KCHS", iata: "CHS", name: "Charleston International", city: "Charleston", state: "SC" },
    { icao: "KCMH", iata: "CMH", name: "John Glenn Columbus International", city: "Columbus", state: "OH" },
    { icao: "KCOS", iata: "COS", name: "Colorado Springs Airport", city: "Colorado Springs", state: "CO" },
    { icao: "KDAL", iata: "DAL", name: "Dallas Love Field", city: "Dallas", state: "TX" },
    { icao: "KDSM", iata: "DSM", name: "Des Moines International", city: "Des Moines", state: "IA" },
    { icao: "KELP", iata: "ELP", name: "El Paso International", city: "El Paso", state: "TX" },
    { icao: "KGSO", iata: "GSO", name: "Piedmont Triad International", city: "Greensboro", state: "NC" },
    { icao: "KGRR", iata: "GRR", name: "Gerald R Ford International", city: "Grand Rapids", state: "MI" },
    { icao: "KGSP", iata: "GSP", name: "Greenville-Spartanburg International", city: "Greenville", state: "SC" },
    { icao: "KIND", iata: "IND", name: "Indianapolis International", city: "Indianapolis", state: "IN" },
    { icao: "KJAX", iata: "JAX", name: "Jacksonville International", city: "Jacksonville", state: "FL" },
    { icao: "KLIT", iata: "LIT", name: "Bill and Hillary Clinton National", city: "Little Rock", state: "AR" },
    { icao: "KMHT", iata: "MHT", name: "Manchester-Boston Regional", city: "Manchester", state: "NH" },
    { icao: "KOKC", iata: "OKC", name: "Will Rogers World", city: "Oklahoma City", state: "OK" },
    { icao: "KOMA", iata: "OMA", name: "Eppley Airfield", city: "Omaha", state: "NE" },
    { icao: "KORF", iata: "ORF", name: "Norfolk International", city: "Norfolk", state: "VA" },
    { icao: "KPVD", iata: "PVD", name: "T F Green International", city: "Providence", state: "RI" },
    { icao: "KRIC", iata: "RIC", name: "Richmond International", city: "Richmond", state: "VA" },
    { icao: "KRNO", iata: "RNO", name: "Reno-Tahoe International", city: "Reno", state: "NV" },
    { icao: "KROC", iata: "ROC", name: "Greater Rochester International", city: "Rochester", state: "NY" },
    { icao: "KSAV", iata: "SAV", name: "Savannah/Hilton Head International", city: "Savannah", state: "GA" },
    { icao: "KSYR", iata: "SYR", name: "Syracuse Hancock International", city: "Syracuse", state: "NY" },
    { icao: "KTUL", iata: "TUL", name: "Tulsa International", city: "Tulsa", state: "OK" },

    // Missing States - Complete US Coverage
    // Mississippi (MS)
    { icao: "KJAN", iata: "JAN", name: "Jackson-Medgar Wiley Evers International", city: "Jackson", state: "MS" },
    { icao: "KGPT", iata: "GPT", name: "Gulfport-Biloxi International", city: "Gulfport", state: "MS" },
    // Maine (ME)
    { icao: "KPWM", iata: "PWM", name: "Portland International Jetport", city: "Portland", state: "ME" },
    { icao: "KBGR", iata: "BGR", name: "Bangor International", city: "Bangor", state: "ME" },
    // Montana (MT)
    { icao: "KBIL", iata: "BIL", name: "Billings Logan International", city: "Billings", state: "MT" },
    { icao: "KBZN", iata: "BZN", name: "Bozeman Yellowstone International", city: "Bozeman", state: "MT" },
    { icao: "KMSO", iata: "MSO", name: "Missoula Montana Airport", city: "Missoula", state: "MT" },
    // North Dakota (ND)
    { icao: "KFAR", iata: "FAR", name: "Hector International", city: "Fargo", state: "ND" },
    { icao: "KBIS", iata: "BIS", name: "Bismarck Airport", city: "Bismarck", state: "ND" },
    // South Dakota (SD)
    { icao: "KFSD", iata: "FSD", name: "Sioux Falls Regional", city: "Sioux Falls", state: "SD" },
    { icao: "KRAP", iata: "RAP", name: "Rapid City Regional", city: "Rapid City", state: "SD" },
    // Vermont (VT)
    { icao: "KBTV", iata: "BTV", name: "Burlington International", city: "Burlington", state: "VT" },
    // West Virginia (WV)
    { icao: "KCRW", iata: "CRW", name: "Yeager Airport", city: "Charleston", state: "WV" },
    // Wyoming (WY)
    { icao: "KJAC", iata: "JAC", name: "Jackson Hole Airport", city: "Jackson", state: "WY" },
    { icao: "KCPR", iata: "CPR", name: "Casper-Natrona County International", city: "Casper", state: "WY" },
    // Kansas (KS)
    { icao: "KICT", iata: "ICT", name: "Wichita Dwight D Eisenhower National", city: "Wichita", state: "KS" },
    // Additional Regional Airports
    { icao: "KLEX", iata: "LEX", name: "Blue Grass Airport", city: "Lexington", state: "KY" },
    { icao: "KCHA", iata: "CHA", name: "Chattanooga Metropolitan", city: "Chattanooga", state: "TN" },
    { icao: "KTYS", iata: "TYS", name: "McGhee Tyson Airport", city: "Knoxville", state: "TN" },
    { icao: "KMOB", iata: "MOB", name: "Mobile Regional", city: "Mobile", state: "AL" },
    { icao: "KHSV", iata: "HSV", name: "Huntsville International", city: "Huntsville", state: "AL" },
    { icao: "KPNS", iata: "PNS", name: "Pensacola International", city: "Pensacola", state: "FL" },
    { icao: "KVPS", iata: "VPS", name: "Destin-Fort Walton Beach", city: "Fort Walton Beach", state: "FL" },
    { icao: "KECP", iata: "ECP", name: "Northwest Florida Beaches International", city: "Panama City", state: "FL" },
    { icao: "KDAY", iata: "DAY", name: "James M Cox Dayton International", city: "Dayton", state: "OH" },
    { icao: "KCAK", iata: "CAK", name: "Akron-Canton Airport", city: "Akron", state: "OH" },
    { icao: "KFWA", iata: "FWA", name: "Fort Wayne International", city: "Fort Wayne", state: "IN" },
    { icao: "KSBN", iata: "SBN", name: "South Bend International", city: "South Bend", state: "IN" },
    { icao: "KLAN", iata: "LAN", name: "Capital Region International", city: "Lansing", state: "MI" },
    { icao: "KFNT", iata: "FNT", name: "Bishop International", city: "Flint", state: "MI" },
    { icao: "KAZO", iata: "AZO", name: "Kalamazoo/Battle Creek International", city: "Kalamazoo", state: "MI" },
    { icao: "KMSN", iata: "MSN", name: "Dane County Regional", city: "Madison", state: "WI" },
    { icao: "KGRB", iata: "GRB", name: "Green Bay Austin Straubel International", city: "Green Bay", state: "WI" },
    { icao: "KSPI", iata: "SPI", name: "Abraham Lincoln Capital", city: "Springfield", state: "IL" },
    { icao: "KMLI", iata: "MLI", name: "Quad City International", city: "Moline", state: "IL" },
    { icao: "KPSP", iata: "PSP", name: "Palm Springs International", city: "Palm Springs", state: "CA" },
    { icao: "KFAT", iata: "FAT", name: "Fresno Yosemite International", city: "Fresno", state: "CA" },
    { icao: "KSBP", iata: "SBP", name: "San Luis Obispo County Regional", city: "San Luis Obispo", state: "CA" },
    { icao: "KGEG", iata: "GEG", name: "Spokane International", city: "Spokane", state: "WA" },
    { icao: "KBLI", iata: "BLI", name: "Bellingham International", city: "Bellingham", state: "WA" },
    { icao: "KEUG", iata: "EUG", name: "Eugene Airport", city: "Eugene", state: "OR" },
    { icao: "KMFR", iata: "MFR", name: "Rogue Valley International-Medford", city: "Medford", state: "OR" },
    { icao: "KFCA", iata: "FCA", name: "Glacier Park International", city: "Kalispell", state: "MT" },
    { icao: "KIDA", iata: "IDA", name: "Idaho Falls Regional", city: "Idaho Falls", state: "ID" },
    { icao: "KTWF", iata: "TWF", name: "Magic Valley Regional", city: "Twin Falls", state: "ID" },

    // Pacific Territories (P* prefix)
    { icao: "PHNL", iata: "HNL", name: "Daniel K Inouye International", city: "Honolulu", state: "HI" },
    { icao: "PHOG", iata: "OGG", name: "Kahului Airport", city: "Kahului", state: "HI" },
    { icao: "PHKO", iata: "KOA", name: "Ellison Onizuka Kona International", city: "Kona", state: "HI" },
    { icao: "PHLI", iata: "LIH", name: "Lihue Airport", city: "Lihue", state: "HI" },
    { icao: "PANC", iata: "ANC", name: "Ted Stevens Anchorage International", city: "Anchorage", state: "AK" },
    { icao: "PAFA", iata: "FAI", name: "Fairbanks International", city: "Fairbanks", state: "AK" },
    { icao: "PAJN", iata: "JNU", name: "Juneau International", city: "Juneau", state: "AK" },
    { icao: "PGUM", iata: "GUM", name: "Antonio B Won Pat International", city: "Guam", state: "GU" },

    // Caribbean Territories
    { icao: "TJSJ", iata: "SJU", name: "Luis Munoz Marin International", city: "San Juan", state: "PR" },
    { icao: "TJBQ", iata: "BQN", name: "Rafael Hernandez International", city: "Aguadilla", state: "PR" },
    { icao: "TJPS", iata: "PSE", name: "Mercedita International", city: "Ponce", state: "PR" },
    { icao: "TIST", iata: "STT", name: "Cyril E King Airport", city: "Charlotte Amalie", state: "VI" },
    { icao: "TISX", iata: "STX", name: "Henry E Rohlsen Airport", city: "Christiansted", state: "VI" },

    // Northern Mariana Islands
    { icao: "PGSN", iata: "SPN", name: "Saipan International", city: "Saipan", state: "MP" },
];

// State name to code mapping for search
const STATE_NAMES = {
    'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
    'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
    'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
    'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
    'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
    'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
    'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
    'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
    'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
    'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
    'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
    'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
    'wisconsin': 'WI', 'wyoming': 'WY', 'district of columbia': 'DC',
    // US Territories
    'guam': 'GU', 'puerto rico': 'PR', 'virgin islands': 'VI', 'us virgin islands': 'VI',
    'northern mariana islands': 'MP', 'american samoa': 'AS', 'saipan': 'MP',
};

// Search function - matches IATA, ICAO, name, city, or state name
function searchAirports(query) {
    if (!query || query.length < 2) return [];

    const q = query.toLowerCase().trim();

    // Check if query matches a state name, get the code
    const stateCode = STATE_NAMES[q] || Object.entries(STATE_NAMES)
        .find(([name]) => name.startsWith(q))?.[1];

    return US_AIRPORTS
        .filter(apt =>
            apt.icao.toLowerCase().includes(q) ||
            apt.iata.toLowerCase().includes(q) ||
            apt.name.toLowerCase().includes(q) ||
            apt.city.toLowerCase().includes(q) ||
            apt.state.toLowerCase() === q ||
            (stateCode && apt.state === stateCode) ||
            `${apt.city} ${apt.state}`.toLowerCase().includes(q)
        )
        .slice(0, 10)  // Limit to 10 results
        .map(apt => ({
            icao: apt.icao,
            iata: apt.iata,
            display: `${apt.iata} - ${apt.name}`,
            subtitle: `${apt.city}, ${apt.state}`
        }));
}

// Get airport by ICAO or IATA
function getAirportByCode(code) {
    const c = code.toUpperCase();
    return US_AIRPORTS.find(apt =>
        apt.icao.toUpperCase() === c || apt.iata.toUpperCase() === c
    );
}

// Legacy function for compatibility
function getAirportByIcao(icao) {
    return getAirportByCode(icao);
}

// Export for use
window.searchAirports = searchAirports;
window.getAirportByIcao = getAirportByIcao;
window.getAirportByCode = getAirportByCode;
window.US_AIRPORTS = US_AIRPORTS;
