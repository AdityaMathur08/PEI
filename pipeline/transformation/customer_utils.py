import re

def fix_customer_name_string(input_string:str)->str:

    if input_string is None:
        return ""
    
    else:

        # replace @ with a and  "ä" with 'ae' 
        input_string = input_string.replace('@','a').replace("ä",'ae')


        # Replace non-alphabetic characters and consecutive spaces with a single space
        

        cleaned_string = re.sub(r'[^a-zA-Z\s\']+|(?<=\s)\s+', ' ', input_string)
        
        # Remove leading and trailing spaces
        cleaned_string = cleaned_string.strip()
        cleaned_string = cleaned_string.replace(' ','')
        # Add space between camel case
        cleaned_string = re.sub(r'([a-z])([A-Z])', r'\1 \2', cleaned_string)
        cleaned_string = cleaned_string.replace("'","")
        # Add space between consequtive upper case
        cleaned_string = re.sub(r'([A-Z])([A-Z])', r'\1 \2', cleaned_string)
        return cleaned_string



def fix_phone_number_string(input_string:str)->str:
    # Remove non-numeric characters
    cleaned_string = re.sub(r'[^0-9]', '', input_string)
    # # Remove leading 1
    # if cleaned_string.startswith('1'):
    #     cleaned_string = cleaned_string[1:]
    # # Add leading 1
    # cleaned_string = '1' + cleaned_string
    cleaned_string = cleaned_string[:10]
    if len(cleaned_string) == 10:
        return cleaned_string  
    else:
        return ""
