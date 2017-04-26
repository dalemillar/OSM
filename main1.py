import csv
import codecs
import re
import xml.etree.cElementTree as ET

import cerberus

import schema

OSM_PATH = "C:/Users/dmillar/Documents/Udacity/Nanodegree_Data_Analyst/OpenStreetMapProject/oxford_england.osm"
NODES_PATH = "C:/Users/dmillar/Documents/Udacity/Nanodegree_Data_Analyst/OpenStreetMapProject/nodes.csv"
NODE_TAGS_PATH = "C:/Users/dmillar/Documents/Udacity/Nanodegree_Data_Analyst/OpenStreetMapProject/nodes_tags.csv"
WAYS_PATH = "C:/Users/dmillar/Documents/Udacity/Nanodegree_Data_Analyst/OpenStreetMapProject/ways.csv"
WAY_NODES_PATH = "C:/Users/dmillar/Documents/Udacity/Nanodegree_Data_Analyst/OpenStreetMapProject/ways_nodes.csv"
WAY_TAGS_PATH = "C:/Users/dmillar/Documents/Udacity/Nanodegree_Data_Analyst/OpenStreetMapProject/ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

expected=['Road','Lane','Street','Close','Way','Green', 'Avenue', 'Turn', 'Mead', 'Drive']
way_dict={'rd':'Road','st':'Street','ave':'Avenue','drv':'Drive'}


def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements

 
    if element.tag == 'node':  
        for field in NODE_FIELDS:
            if field in element.attrib:
                node_attribs[field]=element.attrib[field]
        tags=secondary_tags(element)
        return {'node': node_attribs, 'node_tags': tags}
    elif element.tag == 'way':
        for field in WAY_FIELDS:
            if field in element.attrib:
                way_attribs[field]=element.attrib[field]
        tags=secondary_tags(element)
        counter=0
        for node in element.iter('nd'):
            temp_dict={'id':element.attrib['id'],'position':counter,'node_id':node.attrib['ref']}
            way_nodes.append(temp_dict)
            counter+=1          
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}


# ================================================== #
#               Helper Functions                     #
# ================================================== #
def last_word_address_fixer(sentence):
    
#Input a physical address, strip off the last word,
#if it's not in 'expected' list but is in dictioinary for
#abbreviations of road names, return with unabbreviated
#road name.  If not recognized, return in original form

    sentence=sentence.rstrip('.')
    last=re.compile(r'\s(\w+)$')
    
    word = last.search(sentence)
    try:
        word = word.group().lstrip()
        if word not in expected:
            if word.lower() in way_dict:
                return sentence.replace(word,way_dict[word.lower()])
            else:
                return sentence
    except:
        return sentence
    
def post_coder(post_code):
    
#Check to see if proper post code.  If it is, return as is.  If not, return
#empty string

    proper_code=re.compile(r'\b[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][ABD-HJLNP-UW-Z]{2}\b')
    if proper_code.search(post_code):
        return post_code
    else:
        return ''

def secondary_tags(element):

#Parse for sub-elements in element called tag.  Construct dictionary for tag in
#temp_dict as per instructions using 'tagger'.  If 'key' key vale is a post code, clean the
#post code using 'post_coder'.  If 'key' key value is a street, check for abbreviations in last
#word and correct.

    temp_list=[]
    for tag in element.iter('tag'):
            if re.search(PROBLEMCHARS,tag.attrib['k']):
                pass
            else:
                temp_dict=tagger(tag,element.attrib['id'])
            if temp_dict['key']=='postcode' or temp_dict['key']=='postal_code'\
               or temp_dict['key']=='uk_postcode':
                temp_dict['value']=post_coder(temp_dict['value'])
            if temp_dict['key']=='street' or temp_dict['key']=='naptan:Street':
                temp_dict['value']=last_word_address_fixer(temp_dict['value'])
	
            temp_list.append(temp_dict)
    return temp_list   
 
def tagger(tag,node_id):
    temp_dict={'id':node_id,'type':'regular','value':tag.attrib['v']}
    if re.search(LOWER_COLON,tag.attrib['k']):
        word1,word2=splitter(tag.attrib['k'])
        temp_dict['type']=word1
        temp_dict['key']=word2
    else:
        temp_dict['key']=tag.attrib['k']
    return temp_dict

def splitter(word):
    colon =word.index(':')
    word1=word[:colon]
    word2=word[colon+1:]
    return word1,word2    

def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_strings = (
            "{0}: {1}".format(k, v if isinstance(v, str) else ", ".join(v))
            for k, v in errors.iteritems()
        )
        raise cerberus.ValidationError(
            message_string.format(field, "\n".join(error_strings))
        )


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file, \
         codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file, \
         codecs.open(WAYS_PATH, 'w') as ways_file, \
         codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file, \
         codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])

        
if __name__ == '__main__':
    # Note: Validation is ~ 10X slower. For the project consider using a small
    # sample of the map when validating.
    process_map(OSM_PATH, validate=False)
