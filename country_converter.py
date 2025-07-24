class CountryConverter:
    # Dictionnaire de conversion ISO alpha-3 -> Nom complet
    ISO_TO_COUNTRY = {
        'HKG': 'Hong Kong', 
        'MAC': 'Macao',      
        'TWN': 'Taïwan',
        'ROM': 'Roumanie',        
        'MUS': 'Maurice',         
        'MUR': 'Maurice',
        'ZAF': 'Afrique du Sud', 'AFG': 'Afghanistan', 'ALB': 'Albanie', 'DZA': 'Algérie',
        'DEU': 'Allemagne', 'AND': 'Andorre', 'AGO': 'Angola', 'ATG': 'Antigua-et-Barbuda',
        'SAU': 'Arabie Saoudite', 'ARG': 'Argentine', 'ARM': 'Arménie', 'AUS': 'Australie',
        'AUT': 'Autriche', 'AZE': 'Azerbaïdjan', 'BHS': 'Bahamas', 'BHR': 'Bahreïn',
        'BGD': 'Bangladesh', 'BRB': 'Barbade', 'BEL': 'Belgique', 'BLZ': 'Belize',
        'BEN': 'Bénin', 'BTN': 'Bhoutan', 'BLR': 'Biélorussie', 'MMR': 'Myanmar',
        'BOL': 'Bolivie', 'BIH': 'Bosnie-Herzégovine', 'BWA': 'Botswana', 'BRA': 'Brésil',
        'BRN': 'Brunei', 'BGR': 'Bulgarie', 'BFA': 'Burkina Faso', 'BDI': 'Burundi',
        'KHM': 'Cambodge', 'CMR': 'Cameroun', 'CAN': 'Canada', 'CPV': 'Cap Vert',
        'CHL': 'Chili', 'CHN': 'Chine', 'CYP': 'Chypre', 'COL': 'Colombie',
        'COM': 'Comores', 'PRK': 'Corée du Nord', 'KOR': 'Corée du Sud', 'CRI': 'Costa Rica',
        'CIV': "Côte d'Ivoire", 'HRV': 'Croatie', 'CUB': 'Cuba', 'DNK': 'Danemark',
        'DJI': 'Djibouti', 'DMA': 'Dominique', 'EGY': 'Égypte', 'ARE': 'Émirats arabes unis',
        'ECU': 'Équateur', 'ERI': 'Érythrée', 'ESP': 'Espagne', 'SWZ': 'Eswatini',
        'EST': 'Estonie', 'USA': 'États-Unis', 'ETH': 'Éthiopie', 'FJI': 'Fidji',
        'FIN': 'Finlande', 'FRA': 'France', 'GAB': 'Gabon', 'GMB': 'Gambie',
        'GEO': 'Géorgie', 'GHA': 'Ghana', 'GRC': 'Grèce', 'GRD': 'Grenade',
        'GTM': 'Guatemala', 'GIN': 'Guinée', 'GNQ': 'Guinée équatoriale', 'GNB': 'Guinée-Bissau',
        'GUY': 'Guyane', 'HTI': 'Haïti', 'HND': 'Honduras', 'HUN': 'Hongrie',
        'COK': 'Îles Cook', 'MHL': 'Îles Marshall', 'IND': 'Inde', 'IDN': 'Indonésie',
        'IRQ': 'Irak', 'IRN': 'Iran', 'IRL': 'Irlande', 'ISL': 'Islande',
        'ISR': 'Israël', 'ITA': 'Italie', 'JAM': 'Jamaïque', 'JPN': 'Japon',
        'JOR': 'Jordanie', 'KAZ': 'Kazakhstan', 'KEN': 'Kenya', 'KGZ': 'Kirghizistan',
        'KIR': 'Kiribati', 'KWT': 'Koweït', 'LAO': 'Laos', 'LSO': 'Lesotho',
        'LVA': 'Lettonie', 'LBN': 'Liban', 'LBR': 'Libéria', 'LBY': 'Libye',
        'LIE': 'Liechtenstein', 'LTU': 'Lituanie', 'LUX': 'Luxembourg', 'MKD': 'Macédoine',
        'MDG': 'Madagascar', 'MYS': 'Malaisie', 'MWI': 'Malawi', 'MDV': 'Maldives',
        'MLI': 'Mali', 'MLT': 'Malte', 'MAR': 'Maroc',
        'MRT': 'Mauritanie', 'MEX': 'Mexique', 'FSM': 'Micronésie', 'MDA': 'Moldavie',
        'MCO': 'Monaco', 'MNG': 'Mongolie', 'MNE': 'Monténégro', 'MOZ': 'Mozambique',
        'NAM': 'Namibie', 'NRU': 'Nauru', 'NPL': 'Népal', 'NIC': 'Nicaragua',
        'NER': 'Niger', 'NGA': 'Nigéria', 'NIU': 'Niue', 'NOR': 'Norvège',
        'NZL': 'Nouvelle-Zélande', 'OMN': 'Oman', 'UGA': 'Ouganda', 'UZB': 'Ouzbékistan',
        'PAK': 'Pakistan', 'PLW': 'Palaos', 'PSE': 'Palestine', 'PAN': 'Panama',
        'PNG': 'Papouasie-Nouvelle-Guinée', 'PRY': 'Paraguay', 'NLD': 'Pays-Bas', 'PER': 'Pérou',
        'PHL': 'Philippines', 'POL': 'Pologne', 'PRT': 'Portugal', 'QAT': 'Qatar',
        'CAF': 'République centrafricaine', 'COD': 'République démocratique du Congo',
        'DOM': 'République Dominicaine', 'COG': 'République du Congo', 'CZE': 'République Tchèque',
        'ROU': 'Roumanie', 'GBR': 'Royaume-Uni', 'RUS': 'Russie', 'RWA': 'Rwanda',
        'KNA': 'Saint-Kitts-et-Nevis', 'VCT': 'Saint-Vincent-et-les-Grenadines',
        'LCA': 'Sainte-Lucie', 'SMR': 'Saint Marin', 'SLB': 'Îles Salomon',
        'SLV': 'Le Salvador', 'WSM': 'Samoa', 'STP': 'Sao Tomé-et-Principe',
        'SEN': 'Sénégal', 'SRB': 'Serbie', 'SYC': 'Seychelles', 'SLE': 'Sierra Leone',
        'SGP': 'Singapour', 'SVK': 'Slovaquie', 'SVN': 'Slovénie', 'SOM': 'Somalie',
        'SDN': 'Soudan', 'SSD': 'Soudan du Sud', 'LKA': 'Sri Lanka', 'SWE': 'Suède',
        'CHE': 'Suisse', 'SUR': 'Suriname', 'SYR': 'Syrie', 'TJK': 'Tajikistan',
        'TZA': 'Tanzanie', 'TCD': 'Tchad', 'THA': 'Thaïlande', 'TLS': 'Timor oriental',
        'TGO': 'Togo', 'TON': 'Tonga', 'TTO': 'Trinité-et-Tobago', 'TUN': 'Tunisie',
        'TKM': 'Turkménistan', 'TUR': 'Turquie', 'TUV': 'Tuvalu', 'UKR': 'Ukraine',
        'URY': 'Uruguay', 'VUT': 'Vanuatu', 'VAT': 'Vatican', 'VEN': 'Vénézuela',
        'VNM': 'Vietnam', 'YEM': 'Yemen', 'ZMB': 'Zambie', 'ZWE': 'Zimbabwe'
    }
    
    @classmethod
    def convert_iso_to_country(cls, iso_code):
        if not iso_code or not isinstance(iso_code, str):
            return iso_code
            
        iso_code = iso_code.strip().upper()
        return cls.ISO_TO_COUNTRY.get(iso_code, iso_code)
    
    @classmethod
    def convert_country_to_iso(cls, country_name):
        if not country_name or not isinstance(country_name, str):
            return country_name
            
        for iso, name in cls.ISO_TO_COUNTRY.items():
            if name.lower() == country_name.lower().strip():
                return iso
        return country_name
    
    @classmethod
    def get_all_countries(cls):
        return list(cls.ISO_TO_COUNTRY.values())
    
    @classmethod
    def get_all_iso_codes(cls):
        return list(cls.ISO_TO_COUNTRY.keys())
