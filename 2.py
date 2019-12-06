import json
import re

from dateutil import parser
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

try:
    spark
except NameError:
    spark = SparkSession.builder.appName("proj").getOrCreate()

######################## Utils ########################

def count_person_name(dataset):
    ret = count


def count_business_name(dataset):
    ret = count


def count_phone_number(dataset):
    phone_regex = re.compile('(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})')
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if phone_regex.search(x[0]) else (x[0], 0)).values().sum()
    return count


def count_address(dataset):
    ret = count


def count_street_name(dataset):
    ret = count


def count_city(dataset):    # most > 0.7, 0.6 x1, 0.5x1, 0.3x1
    # https://data.ny.gov/Government-Finance/New-York-State-Locality-Hierarchy-with-Websites/55k6-h6qq/data
    cities = ['albany', 'montgomery', 'cayuga', 'genesee', 'dutchess', 'broome', 'bronx', 'kings', 'erie', 'ontario', 'steuben', 'cortland', 'chautauqua', 'chemung', 'oswego', 'nassau', 'warren', 'fulton', 'columbia', 'tompkins', 'ulster', 'herkimer', 'niagara', 'new york', 'saratoga', 'orange', 'westchester', 'new york city', 'chenango', 'st lawrence', 'cattaraugus', 'madison', 'otsego', 'clinton', 'queens', 'rensselaer', 'monroe', 'oneida', 'schenectady', 'richmond', 'onondaga', 'jefferson', 'allegany', 'lewis', 'delaware', 'essex', 'franklin', 'greene', 'hamilton', 'wayne', 'livingston', 'orleans', 'putnam', 'rockland', 'schoharie', 'schuyler', 'seneca', 'suffolk', 'sullivan', 'tioga', 'washington', 'wyoming', 'yates', 'amsterdam', 'auburn', 'batavia', 'beacon', 'binghamton', 'brooklyn', 'buffalo', 'canandaigua', 'cohoes', 'corning', 'dunkirk', 'elmira', 'geneva', 'glen cove', 'glens falls', 'gloversville', 'hornell', 'hudson', 'ithaca', 'jamestown', 'johnstown', 'kingston', 'lackawanna', 'little falls', 'lockport', 'long beach', 'manhattan', 'mechanicville', 'middletown', 'mt vernon', 'newburgh', 'new rochelle', 'niagara falls', 'north tonawanda', 'norwich', 'ogdensburg', 'olean', 'oneonta', 'peekskill', 'plattsburgh', 'port jervis', 'poughkeepsie', 'rochester', 'rome', 'rye', 'salamanca', 'saratoga springs', 'sherrill', 'staten island', 'syracuse', 'tonawanda', 'troy', 'utica', 'watertown', 'watervliet', 'white plains', 'yonkers', 'berne', 'bethlehem', 'coeymans', 'colonie', 'green island', 'guilderland', 'knox', 'new scotland', 'rensselaerville', 'westerlo', 'alfred', 'allen', 'alma', 'almond', 'amity', 'andover', 'angelica', 'belfast', 'birdsall', 'bolivar', 'burns', 'caneadea', 'centerville', 'clarksville', 'cuba', 'friendship', 'granger', 'grove', 'hume', 'independence', 'new hudson', 'rushford', 'scio', 'ward', 'wellsville', 'west almond', 'willing', 'wirt', 'barker', 'colesville', 'conklin', 'dickinson', 'fenton', 'kirkwood', 'lisle', 'maine', 'nanticoke', 'sanford', 'triangle', 'union', 'vestal', 'windsor', 'ashford', 'carrollton', 'coldspring', 'conewango', 'dayton', 'east otto', 'ellicottville', 'farmersville', 'franklinville', 'freedom', 'great valley', 'hinsdale', 'humphrey', 'ischua', 'leon', 'little valley', 'lyndon', 'machias', 'mansfield', 'napoli', 'new albion', 'otto', 'perrysburg', 'persia', 'portville', 'randolph', 'red house', 'south valley', 'yorkshire', 'aurelius', 'brutus', 'cato', 'conquest', 'fleming', 'genoa', 'ira', 'ledyard', 'locke', 'mentz', 'montezuma', 'moravia', 'niles', 'owasco', 'scipio', 'sempronius', 'sennett', 'springport', 'sterling', 'summerhill', 'throop', 'venice', 'victory', 'arkwright', 'busti', 'carroll', 'charlotte', 'cherry creek', 'clymer', 'ellery', 'ellicott', 'ellington', 'french creek', 'gerry', 'hanover', 'harmony', 'kiantone', 'turin', 'mina', 'north harmony', 'poland', 'pomfret', 'portland', 'ripley', 'sheridan', 'sherman', 'stockton', 'villenova', 'westfield', 'ashland', 'baldwin', 'big flats', 'catlin', 'erin', 'horseheads', 'southport', 'van etten', 'veteran', 'afton', 'bainbridge', 'columbus', 'coventry', 'german', 'guilford', 'lincklaen', 'mc donough', 'new berlin', 'north norwich', 'otselic', 'oxford', 'pharsalia', 'pitcher', 'plymouth', 'preston', 'sherburne', 'smithville', 'smyrna', 'altona', 'ausable', 'beekmantown', 'black brook', 'champlain', 'chazy', 'dannemora', 'ellenburg', 'mooers', 'peru', 'saranac', 'schuyler falls', 'ancram', 'austerlitz', 'canaan', 'chatham', 'claverack', 'clermont', 'copake', 'gallatin', 'germantown', 'ghent', 'greenport', 'hillsdale', 'kinderhook', 'new lebanon', 'stockport', 'stuyvesant', 'taghkanic', 'cincinnatus', 'cortlandville', 'cuyler', 'freetown', 'harford', 'homer', 'lapeer', 'marathon', 'preble', 'scott', 'solon', 'taylor', 'truxton', 'virgil', 'willet', 'andes', 'bovina', 'colchester', 'davenport', 'delhi', 'deposit', 'hamden', 'hancock', 'harpersfield', 'kortright', 'masonville', 'meredith', 'roxbury', 'sidney', 'stamford', 'walton', 'amenia', 'beekman', 'dover', 'east fishkill', 'fishkill', 'hyde park', 'la grange', 'milan', 'northeast', 'pawling', 'pine plains', 'pleasant valley', 'red hook', 'stanford', 'wappinger', 'alden', 'amherst', 'aurora', 'boston', 'brant', 'cheektowaga', 'clarence', 'colden', 'collins', 'concord', 'eden', 'elma', 'evans', 'grand island', 'hamburg', 'holland', 'lancaster', 'marilla', 'newstead', 'north collins', 'orchard park', 'sardinia', 'wales', 'west seneca', 'chesterfield', 'crown point', 'elizabethtown', 'jay', 'keene', 'minerva', 'moriah', 'newcomb', 'north elba', 'north hudson', 'schroon', 'st. armand', 'ticonderoga', 'westport', 'willsboro', 'wilmington', 'bangor', 'bellmont', 'bombay', 'brandon', 'brighton', 'burke', 'chateaugay', 'constable', 'duane', 'fort covington', 'harrietstown', 'malone', 'moira', 'santa clara', 'tupper lake', 'waverly', 'westville', 'bleecker', 'broadalbin', 'caroga', 'ephratah', 'mayfield', 'northampton', 'oppenheim', 'perth', 'stratford', 'alabama', 'alexander', 'bergen', 'bethany', 'byron', 'darien', 'elba', 'le roy', 'oakfield', 'pavilion', 'pembroke', 'stafford', 'athens', 'cairo', 'catskill', 'coxsackie', 'durham', 'greenville', 'halcott', 'hunter', 'jewett', 'lexington', 'new baltimore', 'prattsville', 'windham', 'arietta', 'benson', 'hope', 'indian lake', 'inlet', 'lake pleasant', 'long lake', 'morehouse', 'wells', 'danube', 'fairfield', 'frankfort', 'german flatts', 'litchfield', 'manheim', 'newport', 'norway', 'ohio', 'russia', 'salisbury', 'stark', 'webb', 'winfield', 'adams', 'alexandria', 'antwerp', 'brownville', 'cape vincent', 'champion', 'clayton', 'ellisburg', 'henderson', 'hounsfield', 'le ray', 'lorraine', 'lyme', 'pamelia', 'philadelphia', 'rodman', 'rutland', 'theresa', 'wilna', 'worth', 'croghan', 'denmark', 'diana', 'greig', 'harrisburg', 'leyden', 'lowville', 'lyonsdale', 'sodus', 'martinsburg', 'montague', 'new bremen', 'osceola', 'pinckney', 'watson', 'west turin', 'avon', 'caledonia', 'conesus', 'geneseo', 'groveland', 'leicester', 'lima', 'livonia', 'mount morris', 'north dansville', 'nunda', 'ossian', 'portage', 'sparta', 'springwater', 'west sparta', 'york', 'brookfield', 'cazenovia', 'de ruyter', 'eaton', 'fenner', 'georgetown', 'lebanon', 'lenox', 'lincoln', 'nelson', 'smithfield', 'stockbridge', 'chili', 'clarkson', 'east rochester', 'gates', 'greece', 'hamlin', 'henrietta', 'irondequoit', 'mendon', 'ogden', 'parma', 'penfield', 'perinton', 'pittsford', 'riga', 'rush', 'sweden', 'webster', 'wheatland', 'canajoharie', 'remsen', 'charleston', 'florida', 'glen', 'minden', 'mohawk', 'palatine', 'root', 'st johnsville', 'hempstead', 'north hempstead', 'oyster bay', 'cambria', 'hartland', 'lewiston', 'newfane', 'pendleton', 'porter', 'royalton', 'somerset', 'wheatfield', 'wilson', 'annsville', 'augusta', 'ava', 'boonville', 'bridgewater', 'camden', 'deerfield', 'florence', 'floyd', 'forestport', 'kirkland', 'lee', 'marcy', 'marshall', 'new hartford', 'paris', 'sangerfield', 'trenton', 'vernon', 'verona', 'vienna', 'western', 'westmoreland', 'whitestown', 'camillus', 'cicero', 'clay', 'dewitt', 'elbridge', 'fabius', 'geddes', 'lafayette', 'lysander', 'manlius', 'marcellus', 'otisco', 'pompey', 'salina', 'skaneateles', 'spafford', 'tully', 'van buren', 'bristol', 'canadice', 'east bloomfield', 'farmington', 'gorham', 'hopewell', 'manchester', 'naples', 'phelps', 'south bristol', 'victor', 'west bloomfield', 'blooming grove', 'chester', 'cornwall', 'crawford', 'deerpark', 'goshen', 'hamptonburgh', 'highlands', 'minisink', 'mount hope', 'new windsor', 'tuxedo', 'wallkill', 'warwick', 'wawayanda', 'woodbury', 'albion', 'barre', 'carlton', 'clarendon', 'gaines', 'kendall', 'murray', 'ridgeway', 'shelby', 'amboy', 'boylston', 'constantia', 'granby', 'hannibal', 'hastings', 'mexico', 'minetto', 'new haven', 'orwell', 'palermo', 'parish', 'redfield', 'richland', 'sandy creek', 'schroeppel', 'scriba', 'volney', 'west monroe', 'williamstown', 'burlington', 'butternuts', 'cherry valley', 'decatur', 'edmeston', 'exeter', 'hartwick', 'laurens', 'maryland', 'middlefield', 'milford', 'morris', 'new lisbon', 'otego', 'pittsfield', 'plainfield', 'richfield', 'roseboom', 'springfield', 'unadilla', 'westford', 'worcester', 'carmel', 'kent', 'patterson', 'philipstown', 'putnam valley', 'southeast', 'berlin', 'brunswick', 'east greenbush', 'grafton', 'hoosick', 'north greenbush', 'petersburgh', 'pittstown', 'poestenkill', 'sand lake', 'schaghticoke', 'schodack', 'stephentown', 'clarkstown', 'haverstraw', 'orangetown', 'ramapo', 'stony point', 'ballston', 'charlton', 'clifton park', 'corinth', 'day', 'edinburg', 'galway', 'greenfield', 'hadley', 'halfmoon', 'malta', 'milton', 'moreau', 'northumberland', 'providence', 'stillwater', 'waterford', 'wilton', 'duanesburg', 'glenville', 'niskayuna', 'princetown', 'rotterdam', 'blenheim', 'carlisle', 'cobleskill', 'conesville', 'esperance', 'gilboa', 'middleburgh', 'richmondville', 'seward', 'sharon', 'summit', 'wright', 'catharine', 'cayuta', 'dix', 'hector', 'montour', 'reading', 'tyrone', 'covert', 'fayette', 'junius', 'lodi', 'ovid', 'romulus', 'seneca falls', 'tyre', 'varick', 'waterloo', 'addison', 'avoca', 'bath', 'bradford', 'cameron', 'campbell', 'canisteo', 'caton', 'cohocton', 'dansville', 'erwin', 'fremont', 'greenwood', 'hartsville', 'hornby', 'hornellsville', 'howard', 'jasper', 'lindley', 'prattsburgh', 'pulteney', 'rathbone', 'thurston', 'troupsburg', 'tuscarora', 'urbana', 'wayland', 'west union', 'wheeler', 'woodhull', 'brasher', 'canton', 'clare', 'clifton', 'colton', 'dekalb', 'de peyster', 'edwards', 'fine', 'fowler', 'gouverneur', 'hammond', 'hermon', 'hopkinton', 'lawrence', 'lisbon', 'louisville', 'macomb', 'madrid', 'massena', 'morristown', 'norfolk', 'oswegatchie', 'parishville', 'piercefield', 'pierrepont', 'pitcairn', 'potsdam', 'rossie', 'russell', 'stockholm', 'waddington', 'babylon', 'brookhaven', 'east hampton', 'huntington', 'islip', 'riverhead', 'shelter island', 'smithtown', 'southampton', 'southold', 'bethel', 'callicoon', 'cochecton', 'fallsburgh', 'forestburgh', 'highland', 'liberty', 'lumberland', 'mamakating', 'neversink', 'thompson', 'tusten', 'barton', 'berkshire', 'candor', 'newark valley', 'nichols', 'owego', 'richford', 'spencer', 'caroline', 'danby', 'dryden', 'enfield', 'groton', 'lansing', 'newfield', 'ulysses', 'denning', 'esopus', 'gardiner', 'hardenburgh', 'hurley', 'lloyd', 'marbletown', 'marlborough', 'new paltz', 'olive', 'plattekill', 'rosendale', 'saugerties', 'shandaken', 'shawangunk', 'wawarsing', 'woodstock', 'bolton', 'hague', 'horicon', 'johnsburg', 'lake george', 'lake luzerne', 'queensbury', 'stony creek', 'thurman', 'warrensburg', 'argyle', 'cambridge', 'dresden', 'easton', 'fort ann', 'fort edward', 'granville', 'greenwich', 'hampton', 'hartford', 'hebron', 'jackson', 'kingsbury', 'salem', 'white creek', 'whitehall', 'arcadia', 'butler', 'galen', 'huron', 'lyons', 'macedon', 'marion', 'palmyra', 'rose', 'savannah', 'walworth', 'williamson', 'wolcott', 'bedford', 'cortlandt', 'eastchester', 'greenburgh', 'harrison', 'lewisboro', 'mamaroneck', 'mount kisco', 'mount pleasant', 'perry', 'new castle', 'north castle', 'north salem', 'ossining', 'pelham', 'pound ridge', 'scarsdale', 'somers', 'yorktown', 'arcade', 'attica', 'bennington', 'castile', 'covington', 'eagle', 'gainesville', 'genesee falls', 'java', 'middlebury', 'orangeville', 'pike', 'sheldon', 'warsaw', 'wethersfield', 'barrington', 'benton', 'italy', 'jerusalem', 'middlesex', 'milo', 'potter', 'starkey', 'torrey', 'rhinebeck', 'union vale', 'ravena', 'menands', 'altamont', 'voorheesville', 'belmont', 'richburg', 'canaseraga', 'port dickinson', 'whitney point', 'endicott', 'johnson city', 'south dayton', 'gowanda', 'delevan', 'weedsport', 'meridian', 'port byron', 'union springs', 'fair haven', 'lakewood', 'sinclairville', 'mayville', 'bemus point', 'celoron', 'falconer', 'forestville', 'silver creek', 'panama', 'fredonia', 'brocton', 'cassadaga', 'wellsburg', 'elmira heights', 'millport', 'earlville', 'keeseville', 'rouses point', 'philmont', 'valatie', 'mc graw', 'fleischmanns', 'margaretville', 'hobart', 'millerton', 'wappingers falls', 'tivoli', 'millbrook', 'williamsville', 'east aurora', 'farnham', 'depew', 'sloan', 'springville', 'angola', 'blasdell', 'akron', 'kenmore', 'port henry', 'lake placid', 'saranac lake', 'brushton', 'northville', 'dolgeville', 'corfu', 'tannersville', 'speculator', 'middleville', 'ilion', 'cold brook', 'west winfield', 'alexandria bay', 'dexter', 'glen park', 'west carthage', 'mannsville', 'sackets harbor', 'black river', 'evans mills', 'chaumont', 'carthage', 'deferiet', 'herrings', 'castorland', 'copenhagen', 'harrisville', 'port leyden', 'lyons falls', 'constableville', 'morrisville', 'canastota', 'wampsville', 'munnsville', 'chittenango', 'honeoye falls', 'spencerport', 'hilton', 'fairport', 'churchville', 'brockport', 'scottsville', 'fort johnson', 'hagaman', 'ames', 'fort plain', 'fultonville', 'fonda', 'nelliston', 'palatine bridge', 'atlantic beach', 'bellerose', 'cedarhurst', 'east rockaway', 'floral park', 'freeport', 'garden city', 'hewlett bay park', 'hewlett harbor', 'hewlett neck', 'island park', 'lynbrook', 'malverne', 'mineola', 'new hyde park', 'rockville centre', 'south floral park', 'stewart manor', 'valley stream', 'woodsburgh', 'baxter estates', 'east hills', 'east williston', 'flower hill', 'great neck', 'great neck estates', 'great neck plaza', 'kensington', 'kings point', 'lake success', 'manor haven', 'munsey park', 'north hills', 'old westbury', 'plandome', 'plandome heights', 'plandome manor', 'port washington north', 'roslyn', 'roslyn estates', 'roslyn harbor', 'russell gardens', 'saddle rock', 'sands point', 'thomaston', 'westbury', 'williston park', 'bayville', 'brookville', 'centre island', 'cove neck', 'farmingdale', 'lattingtown', 'laurel hollow', 'massapequa park', 'matinecock', 'mill neck', 'muttontown', 'old brookville', 'oyster bay cove', 'sea cliff', 'upper brookville', 'middleport', 'youngstown', 'oriskany falls', 'waterville', 'new york mills', 'clayville', 'barneveld', 'holland patent', 'prospect', 'oneida castle', 'sylvan beach', 'oriskany', 'whitesboro', 'yorkville', 'north syracuse', 'east syracuse', 'jordan', 'solvay', 'baldwinsville', 'fayetteville', 'minoa', 'liverpool', 'bloomfield', 'rushville', 'clifton springs', 'shortsville', 'south blooming grove', 'washingtonville', 'cornwall-on-hudson', 'maybrook', 'highland falls', 'unionville', 'harriman', 'kiryas joel', 'walden', 'otisville', 'tuxedo park', 'greenwood lake', 'holley', 'medina', 'lyndonville', 'cleveland', 'central square', 'pulaski', 'lacona', 'phoenix', 'gilbertsville', 'cooperstown', 'richfield springs', 'cold spring', 'nelsonville', 'brewster', 'hoosick falls', 'east nassau', 'valley falls', 'castleton-on-hudson', 'nyack', 'spring valley', 'upper nyack', 'pomona', 'west haverstraw', 'grand view-on-hudson', 'piermont', 'south nyack', 'airmont', 'chestnut ridge', 'hillburn', 'kaser', 'montebello', 'new hempstead', 'new square', 'sloatsburg', 'suffern', 'wesley hills', 'ballston spa', 'round lake', 'south glens falls', 'schuylerville', 'delanson', 'scotia', 'sharon springs', 'odessa', 'montour falls', 'watkins glen', 'burdett', 'interlaken', 'savona', 'riverside', 'south corning', 'painted post', 'arkport', 'north hornell', 'hammondsport', 'rensselaer falls', 'richville', 'norwood', 'heuvelton', 'amityville', 'lindenhurst', 'belle terre', 'bellport', 'lake grove', 'mastic beach', 'old field', 'patchogue', 'poquott', 'port jefferson', 'shoreham', 'sag harbor', 'asharoken', 'huntington bay', 'lloyd harbor', 'northport', 'brightwaters', 'islandia', 'ocean beach', 'saltaire', 'dering harbor', 'head of the harbor', 'nissequogue', 'village of the branch', 'north haven', 'quogue', 'sagaponack', 'westhampton beach', 'west hampton dunes', 'jeffersonville', 'woodridge', 'bloomingburgh', 'wurtsboro', 'monticello', 'freeville', 'cayuga heights', 'trumansburg', 'ellenville', 'hudson falls', 'newark', 'clyde', 'sodus point', 'red creek', 'buchanan', 'croton-on-hudson', 'bronxville', 'tuckahoe', 'ardsley', 'dobbs ferry', 'elmsford', 'hastings-on-hudson', 'irvington', 'tarrytown', 'larchmont', 'briarcliff manor', 'pleasantville', 'sleepy hollow', 'pelham manor', 'port chester', 'rye brook', 'silver springs', 'penn yan', 'dundee']
    def check(city):
        result = [i for i in cities if i in city]
        if len(result) > 0:
            return True
        else:
            return False
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if check(x[0].lower()) else (x[0], 0)).values().sum()
    return count


def count_neighborhood(dataset):    # most > 0.9, 0.8 x2
    # https://en.wikipedia.org/wiki/Neighborhoods_in_New_York_City    
    neighborhoods = ['melrose', 'mott haven', 'port morris', 'hunts point', 'longwood', 'claremont', 'concourse village', 'crotona park', 'morrisania', 'concourse', 'highbridge', 'fordham', 'morris heights', 'mount hope', 'university heights', 'bathgate', 'belmont', 'east tremont', 'west farms', 'bedford park', 'norwood', 'university heights', 'fieldston', 'kingsbridge', 'kingsbridge heights', 'marble hill', 'riverdale', 'spuyten duyvil', 'van cortlandt village', 'bronx river', 'bruckner', 'castle hill', 'clason point', 'harding park', 'parkchester', 'soundview', 'unionport', 'city island', 'co-op city', 'locust point', 'pelham bay', 'silver beach', 'throgs neck', 'westchester square', 'allerton', 'bronxdale', 'indian village', 'laconia', 'morris park', 'pelham gardens', 'pelham parkway', 'van nest', 'baychester', 'edenwald', 'eastchester', 'fish bay', 'olinville', 'wakefield', 'williamsbridge', 'woodlawn', 'greenpoint', 'williamsburg', 'boerum hill', 'brooklyn heights', 'brooklyn navy yard', 'clinton hill', 'dumbo', 'fort greene', 'fulton ferry', 'fulton mall', 'vinegar hill', 'bedford-stuyvesant', 'ocean hill', 'stuyvesant heights', 'bushwick', 'city line', 'cypress hills', 'east new york', 'highland park', 'new lots', 'starrett city', 'carroll gardens', 'cobble hill', 'gowanus', 'park slope', 'red hook', 'greenwood heights', 'sunset park', 'windsor terrace', 'crown heights', 'prospect heights', 'weeksville', 'crown heights', 'prospect lefferts gardens', 'wingate', 'bay ridge', 'dyker heights', 'fort hamilton', 'bath beach', 'bensonhurst', 'gravesend', 'mapleton', 'borough park', 'kensington', 'midwood', 'ocean parkway', 'bensonhurst', 'brighton beach', 'coney island', 'gravesend', 'sea gate', 'flatbush', 'kensington', 'midwood', 'ocean parkway', 'east gravesend', 'gerritsen beach', 'homecrest', 'kings bay', 'kings highway', 'madison', 'manhattan beach', 'plum beach', 'sheepshead bay', 'brownsville', 'ocean hill', 'ditmas village', 'east flatbush', 'erasmus', 'farragut', 'remsen village', 'rugby', 'bergen beach', 'canarsie', 'flatlands', 'georgetown', 'marine park', 'mill basin', 'mill island', 'battery park city', 'financial district', 'tribeca', 'chinatown', 'greenwich village', 'little italy', 'lower east side', 'noho', 'soho', 'west village', 'alphabet city', 'chinatown', 'east village', 'lower east side', 'two bridges', 'chelsea', 'clinton', 'hudson yards', 'midtown', 'gramercy park', 'kips bay', 'rose hill', 'murray hill', 'peter cooper village', 'stuyvesant town', 'sutton place', 'tudor city', 'turtle bay', 'waterside plaza', 'lincoln square', 'manhattan valley', 'upper west side', 'lenox hill', 'roosevelt island', 'upper east side', 'yorkville', 'hamilton heights', 'manhattanville', 'morningside heights', 'harlem', 'polo grounds', 'east harlem', "randall's island", 'spanish harlem', 'wards island', 'inwood', 'washington heights', 'astoria', 'ditmars', 'garden bay', 'long island city', 'old astoria', 'queensbridge', 'ravenswood', 'steinway', 'woodside', 'hunters point', 'long island city', 'sunnyside', 'woodside', 'east elmhurst', 'jackson heights', 'north corona', 'corona', 'elmhurst', 'fresh pond', 'glendale', 'maspeth', 'middle village', 'liberty park', 'ridgewood', 'forest hills', 'rego park', 'bay terrace', 'beechhurst', 'college point', 'flushing', 'linden hill', 'malba', 'queensboro hill', 'whitestone', 'willets point', 'briarwood', 'cunningham heights', 'flushing south', 'fresh meadows', 'hilltop village', 'holliswood', 'jamaica estates', 'kew gardens hills', 'pomonok houses', 'utopia', 'kew gardens', 'ozone park', 'richmond hill', 'woodhaven', 'howard beach', 'lindenwood', 'richmond hill', 'south ozone park', 'tudor village', 'auburndale', 'bayside', 'douglaston', 'east flushing', 'hollis hills', 'little neck', 'oakland gardens', 'baisley park', 'jamaica', 'hollis', 'rochdale village', 'st. albans', 'south jamaica', 'springfield gardens', 'bellerose', 'brookville', 'cambria heights', 'floral park', 'glen oaks', 'laurelton', 'meadowmere', 'new hyde park', 'queens village', 'rosedale', 'arverne', 'bayswater', 'belle harbor', 'breezy point', 'edgemere', 'far rockaway', 'neponsit', 'rockaway park', 'arlington', 'castleton corners', 'clifton', 'concord', 'elm park', 'fort wadsworth', 'graniteville', 'grymes hill', 'livingston', 'mariners harbor', 'meiers corners', 'new brighton', 'port ivory', 'port richmond', 'randall manor', 'rosebank', 'st. george', 'shore acres', 'silver lake', 'stapleton', 'sunnyside', 'tompkinsville', 'west brighton', 'westerleigh', 'arrochar', 'bloomfield', 'bulls head', 'chelsea', 'dongan hills', 'egbertville', 'emerson hill', 'grant city', 'grasmere', 'midland beach', 'new dorp', 'new springville', 'oakwood', 'ocean breeze', 'old town', 'south beach', 'todt hill', 'travis', 'annadale', 'arden heights', 'bay terrace', 'charleston', 'eltingville', 'great kills', 'greenridge', 'huguenot', 'pleasant plains', "prince's bay", 'richmond valley', 'rossville', 'tottenville', 'woodrow']
    def check(neighborhood):
        result = [i for i in neighborhoods if i in neighborhood]
        if len(result) > 0:
            return True
        else:
            return False
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if check(x[0].lower()) else (x[0], 0)).values().sum()
    return coun


def count_coordinates(dataset):
    coordinate_regex = re.compile('(\((-)?\d+(.+)?,( *)?(-)?\d+(.+)?\))')
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if coordinate_regex.search(x[0]) else (x[0], 0)).values().sum()
    return count


def count_zip(dataset):
    zip_regex = re.compile('^[0-9]{5}([- /]?[0-9]{4})?$')
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if zip_regex.search(x[0]) else (x[0], 0)).values().sum()
    return count


def count_borough(dataset):  # 0.92
    boro = ['K', 'M', 'Q', 'R', 'X']
    borough = ['BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND']
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if (x[0].upper() in boro or x[0].upper() in borough) else (x[0], 0)).values().sum()
    return count


def count_school_name(dataset):
    ret = count


def count_color(dataset):   # 0.7
    # https://www.ul.com/resources/color-codes-and-abbreviations-plastics-recognition
    color_list = ['AL', 'AM', 'AO', 'AT', 'BG', 'BK', 'BL', 'BN', 'BZ', 'CH', 'CL', 'CT', 'DK', 'GD', 'GN', 'GT', 'GY', 'IV', 'LT', 'NC', 'OL', 'OP', 'OR', 'PK', 'RD', 'SM', 'TL', 'TN', 'TP', 'VT', 'WT', 'YL']
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if (is_color_like(x[0]) or x[0] in color_list) else (x[0], 0)).values().sum()
    return count


def count_car_make(dataset):
    ret = count


def count_city_agency(dataset):
    ret = count


def count_areas_of_study(dataset):
    # TODO: distinguish from subjects
    area_study = ['ANIMAL SCIENCE', 'ARCHITECTURE', 'BUSINESS', 'COMMUNICATIONS', 'COMPUTER SCIENCE & TECHNOLOGY', \
                'COMPUTER SCIENCE, MATH & TECHNOLOGY', 'COSMETOLOGY', 'CULINARY ARTS', 'ENGINEERING', \
                'ENVIRONMENTAL SCIENCE', 'FILM/VIDEO', 'HEALTH PROFESSIONS', 'HOSPITALITY, TRAVEL, & TOURISM', \
                'HUMANITIES & INTERDISCIPLINARY', 'JROTC', 'LAW & GOVERNMENT', 'PERFORMING ARTS', 'PERFORMING ARTS/VISUAL ART & DESIGN', \
                'PROJECT-BASED LEARNING', 'SCIENCE & MATH', 'TEACHING', 'VISUAL ART & DESIGN', 'ZONED']
    areas = ['general', 'agriculture production and management', 'agricultural economics', 'animal', 'food science', 'plant science and agronomy', 'soil science', 'miscellaneous agriculture', 'forestry', 'natural resources management', 'fine arts', 'drama and theater arts', 'music', 'visual and performing arts', 'commercial art and graphic design', 'film video and photographic arts', 'studio arts', 'miscellaneous fine arts', 'environmental science', 'biology', 'biochemical sciences', 'botany', 'molecular biology', 'ecology', 'genetics', 'microbiology', 'pharmacology', 'physiology', 'zoology', 'neuroscience', 'miscellaneous biology', 'cognitive science and biopsychology', 'general business', 'accounting', 'actuarial science', 'business management and administration', 'operations logistics and e-commerce', 'business economics', 'marketing and marketing research', 'finance', 'human resources and personnel management', 'international business', 'hospitality management', 'management information systems and statistics', 'miscellaneous business & medical administration', 'communications', 'journalism', 'mass media', 'advertising and public relations', 'communication technologies', 'computer and information systems', 'computer programming and data processing', 'computer science', 'information sciences', 'computer administration management and security', 'computer networking and telecommunications', 'mathematics', 'applied mathematics', 'statistics and decision science', 'mathematics and computer science', 'general education', 'educational administration and supervision', 'school student counseling', 'elementary education', 'mathematics teacher education', 'physical and health education teaching', 'early childhood education', 'science and computer teacher education', 'secondary teacher education', 'special needs education', 'social science or history teacher education', 'teacher education: multiple levels', 'language and drama education', 'art and music education', 'miscellaneous education', 'library science', 'architecture', 'general engineering', 'aerospace engineering', 'biological engineering', 'architectural engineering', 'biomedical engineering', 'chemical engineering', 'civil engineering', 'computer engineering', 'electrical engineering', 'engineering mechanics physics and science', 'environmental engineering', 'geological and geophysical engineering', 'industrial and manufacturing engineering', 'materials engineering and materials science', 'mechanical engineering', 'metallurgical engineering', 'mining and mineral engineering', 'naval architecture and marine engineering', 'nuclear engineering', 'petroleum engineering', 'miscellaneous engineering', 'engineering technologies', 'engineering and industrial management', 'electrical engineering technology', 'industrial production technologies', 'mechanical engineering related technologies', 'miscellaneous engineering technologies', 'materials science', 'nutrition sciences', 'general medical and health services', 'communication disorders sciences and services', 'health and medical administrative services', 'medical assisting services', 'medical technologies technicians', 'health and medical preparatory programs', 'nursing', 'pharmacy pharmaceutical sciences and administration', 'treatment therapy professions', 'community and public health', 'miscellaneous health medical professions', 'area ethnic and civilization studies', 'linguistics and comparative language and literature', 'french german latin and other common foreign language studies', 'other foreign languages', 'english language and literature', 'composition and rhetoric', 'liberal arts', 'humanities', 'intercultural and international studies', 'philosophy and religious studies', 'theology and religious vocations', 'anthropology and archeology', 'art history and criticism', 'history', 'united states history', 'cosmetology services and culinary arts', 'family and consumer sciences', 'military technologies', 'physical fitness parks recreation and leisure', 'construction services', 'electrical, mechanical, and precision technologies and production', 'transportation sciences and technologies', 'multi/interdisciplinary studies', 'court reporting', 'pre-law and legal studies', 'criminal justice and fire protection', 'public administration', 'public policy', 'physical sciences', 'astronomy and astrophysics', 'atmospheric sciences and meteorology', 'chemistry', 'geology and earth science', 'geosciences', 'oceanography', 'physics', 'multi-disciplinary or general science', 'nuclear, industrial radiology, and biological technologies', 'psychology', 'educational psychology', 'clinical psychology', 'counseling psychology', 'industrial and organizational psychology', 'social psychology', 'miscellaneous psychology', 'human services and community organization', 'social work', 'interdisciplinary social sciences', 'general social sciences', 'economics', 'criminology', 'geography', 'international relations', 'political science and government', 'sociology', 'miscellaneous social sciences']
    def check(area):
        area = re.sub(r'[^\w ]', '$', area).split('$')
        result = []
        # result = [i for i in areas if (area in i or i in area)]
        for a in area:
            for i in areas:
                if a in i or i in a:
                    result.append(i)
        if len(result) > 0:
            return True
        else:
            return False
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if check(x[0].lower()) else (x[0], 0)).values().sum()
    return count


def count_subjects_in_school(dataset):
    # TODO: modify for core courses
    subjects = ['ENGLISH', 'MATH', 'SCIENCE', 'SOCIAL STUDIES']
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if x[0].upper() in subjects else (x[0], 0)).values().sum()
    return count


def count_school_levels(dataset):
    school_levels = ['ELEMENTARY SCHOOL', 'TRANSFER SCHOOL', 'HIGH SCHOOL TRANSFER', 'TRANSFER HIGH SCHOOL', 'MIDDLE SCHOOL',
                     'K-2 SCHOOL', 'K-3 SCHOOL', 'K-8 SCHOOL', 'YABC', 'D75']

    def check(level):
        result = [i for i in school_levels if level in i]
        if len(result) > 0:
            return True
        else:
            return False
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if check(x[0].upper()) else (x[0], 0)).values().sum()
    return count


def count_university_names(dataset):
    university_names = ['university', 'college']
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if x[0].split(" ")[-1].lower() in university_names else (x[0], 0)).values().sum()
    return count

def count_websites(dataset):
    web_regex = re.compile('(https?:\/\/)?(www\.)?([a-zA-Z0-9]+(-?[a-zA-Z0-9])*\.)+[\w]{2,}(\/\S*)?')
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if web_regex.search(x[0]) else (x[0], 0)).values().sum()
    return count


def count_building_classification(dataset):
    # https://www1.nyc.gov/assets/finance/jump/hlpbldgcode.html
    buildings = ['A0', 'A1', 'A2', 'A3', 'A4', 'A5', 'A6', 'A7', 'A8', 'A9', 'B1', 'B2', 'B3', 'B9', 'C0', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9', 'D0', 'D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7', 'D8', 'D9', 'E1', 'E3', 'E4', 'E5', 'E7', 'E9', 'F1', 'F2', 'F4', 'F5', 'F8', 'F9', 'G0', 'G1', 'G2', 'G3', 'G4', 'G5', 'G6', 'G7', 'G8', 'G9', 'H1', 'H2', 'H3', 'H4', 'H5', 'H6', 'H7', 'H8', 'H9', 'HB', 'HH', 'HR', 'HS', 'I1', 'I2', 'I3', 'I4', 'I5', 'I6', 'I7', 'I9', 'J1', 'J2', 'J3', 'J4', 'J5', 'J6', 'J7', 'J8', 'J9', 'K1', 'K2', 'K3', 'K4', 'K5', 'K6', 'K7', 'K8', 'K9', 'L1', 'L2', 'L3', 'L8', 'L9', 'M1', 'M2', 'M3', 'M4', 'M9', 'N1', 'N2', 'N3', 'N4', 'N9', 'O1', 'O2', 'O3', 'O4', 'O5', 'O6', 'O7', 'O8', 'O9', 'P1', 'P2', 'P3', 'P4', 'P5', 'P6', 'P7', 'P8', 'P9', 'Q0', 'Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7', 'Q8', 'Q9', 'R0', 'R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8', 'R9', 'RA', 'RB', 'RC', 'RD', 'RG', 'RH', 'RI', 'RK', 'RM', 'RR', 'RS', 'RW', 'RX', 'RZ', 'S0', 'S1', 'S2', 'S3', 'S4', 'S5', 'S9', 'T1', 'T2', 'T9', 'U0', 'U1', 'U2', 'U3', 'U4', 'U5', 'U6', 'U7', 'U8', 'U9', 'V0', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9', 'W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9', 'Y1', 'Y2', 'Y3', 'Y4', 'Y5', 'Y6', 'Y7', 'Y8', 'Y9', 'Z0', 'Z1', 'Z2', 'Z3', 'Z4', 'Z5', 'Z6', 'Z7', 'Z8', 'Z9']
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if x[0].split('-')[0] in buildings else (x[0], 0)).values().sum()
    return count

# no available files
def count_vehicle_type(dataset):
    ret = count


def count_type_of_location(dataset):
    ret = count


def count_parks_or_playgrounds(dataset):    # 0.8 x1, 0.3 x1, <0.01 x3
    # TODO:
    words = ['park', 'playground', 'green', 'plots', 'square', 'plaza']
    count = dataset.rdd.map(lambda x: (x[0], x[1]) if x[0].split(" ")[-1].lower() in words else (x[0], 0)).values().sum()
    return count


######################## Main ########################

semantic_types = {
    'person_name': count_person_name,
    'business_name': count_business_name,
    'phone_number': count_phone_number,
    'address': count_address,
    'street_name': count_street_name,
    'city': count_city,
    'neighborhood': count_neighborhood,
    'coordinates': count_coordinates,
    'zip': count_zip,
    'borough': count_borough,
    'school_name': count_school_name,
    'color': count_color,
    'car_make': count_car_make,
    'city_agency': count_city_agency,
    'areas_of_study': count_areas_of_study,
    'subjects_in_school': count_subjects_in_school,
    'university_names': count_university_names,
    'websites': count_websites,
    'building_classification': count_building_classification,
    'vehicle_type': count_vehicle_type,
    'type_of_location': count_type_of_location,
    'parks_or_playgrounds': count_parks_or_playgrounds
}


def profile_semantic(dataset):
    confirm_threshold = 0.95 * dataset.count()
    ret = []
    for semantic_type in semantic_types:
        count, label = semantic_types[semantic_type](dataset), None
        ret.append({'semantic_type': semantic_type, 'label': label, 'count': count})
        if count > confirm_threshold:
            break
    return ret


# 1. list the working subset
with open('./cluster2.txt') as f:
    cluster = json.loads(f.read().replace("'", '"'))

# 2. for each working dataset
for filename in cluster:
    # filename = cluster[0]
    [dataset_name, column_name] = filename.split('.').slice(2)
    print(u'>> entering {}.{}'.format(dataset_name, column_name))
    try:
        with open('task2.{}.{}.json'.format(dataset_name, column_name)) as f:
            json.load(f)
            print(u'>> skipping {}.{}'.format(dataset_name, column_name))
            continue
    except:
        pass

    # 2.1 load dataset
    dataset = (spark.read.format('csv')
               .options(inferschema='true', sep='\t')
               .load('/user/hm74/NYCColumns/{}'.format(filename))
               .toDF('value', 'count'))

    # 2.2 load the corresponding dataset profile
    try:
        with open('task1.{}.json'.format(dataset_name), 'r') as f:
            dataset_profile = json.load(f)

        # 2.3 load the corresponing column profile
        for entry in dataset_profile['columns']:
            if entry['column_name'] == column_name:
                column_profile = entry
                break
        else:
            raise ValueError
    except:
        pass

    # 2.4 create column semantic profile
    output = {
        'column_name': column_name,
        'semantic_types': profile_semantic(dataset)
    }

    # 2.5 dump updated dataset profile as json
    with open('task2.{}.{}.json'.format(dataset_name, column_name), 'w') as f:
        json.dump(output, f, indent=2)
