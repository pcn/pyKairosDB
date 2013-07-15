# -*- python -*-

"""This module contains ease-of-use functions"""

def get_content_values_by_name(content, name):
    """When you've got content, but only want to look at a piece of it,
    specifically the values that are provided for a particular name, then use
    this function.

    Content will look like this when it comes back as json:
        "{\"queries\":
       [
         {\"results\":
           [
             {\"name\":\"test\",
              \"tags\":
                {\"graphite\":[\"yes\"]},
              \"values\":[
                [1373780448859,1.373780448859967E9],
                [1373780450816,1.373780450816504E9],
                [1373781047785,1.373781047785849E9],
                [1373781261751,1.373781261751651E9],
                [1373781264045,1.37378126404582E9],
                [1373781265456,1.373781265456353E9]
              ]
            }
          ]
        }
      ]
    }"

    and this will be turned into a python struct.  This function will
    return the dict that matches the reult whose name and matches the
    argument "name"

    """
    for q in content["queries"]:
        for r in q["results"]:
            if r["name"] == name:
                return r

def content_by_name_substring(content, name):
    """When you've got content, but only want to look at a piece of it,
    specifically the values that are provided for a particular name, then use
    this function.

    Content will look like this when it comes back as json:

    and this will be turned into a python struct.  This function will
    return a list of dicts that matched the provided name.

    :type content: dict
    :param content: The python dict form of the content that has been returned from a query to KairosDB

    :type name: string
    :param name: This is the string that will be used to match, as a lowercase substring, the things that we want to return.

    """
    r_list = list()
    for q in content["queries"]:
        for r in q["results"]:
            if name.lower() in r["name"].lower():
                r_list.append(r)
    return r_list




def get_content_values_by_tag_info(content, tag_key, tag_value_list):
    """When you've got content, but only want to look at a piece of
    it, specifically the values that are provided which match one more
    tag key and some number of tag values, then use this function.

    :type content: dict
    :param content: The python dict form of the content that has been returned from a query to KairosDB

    :type tag_key: list
    :param tag_key: This will index the tag that we're interested in

    :type tag_value_list: list
    :param tag_value_list: list of strings, each of which is a possible tag

    :rtype: list
    :return: a list of result dicts that matched
    """
    r_list = list()
    tag_value_set = frozenset([tv.lower() for tv in tag_value_list])
    for q in content["queries"]:
        for r in q["results"]:
            for t in r["tags"]:
                if tag_key.lower() in [ each_tag.lower() for each_tag in t.keys() ]:
                    # This creates the irritating situation where the tag could have mis-matched
                    # casing - e.g. apple and Apple match by the comparison above.
                    content_tag_set = frozenset([ tv.lower() for tv in t.values])
                    if tag_value_set in content_tag_set:
                        r_list.append(r_list)
    return r_list
