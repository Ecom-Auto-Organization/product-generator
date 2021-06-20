def join_str(*args):
    joined_str = ''
    for arg in args:
        joined_str += str(arg)
    return joined_str

def extract_str (str, delimeter, position):
    str_list = str.split(delimeter)
    return str_list[position]

def get_expression_attr_values(value_dict):
    expression_values = {}
    
    for key, value in value_dict.items():
        expression_values[':' + key] = value
    return expression_values

def get_update_expression(value_dict):
    expression = 'SET'

    for key in value_dict.keys():
        expression = expression + ' ' + key[1:] + '=' + key + ','
    
    return expression[0:len(expression) - 1]