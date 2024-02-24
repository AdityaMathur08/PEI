import re
def round_off_price(price: float) -> float:
    return round(price, 2)

def clean_product_name(product_name):
    product_name = product_name.replace('"', '')
    product_name = product_name.replace('""', ' ')
    product_name = product_name.replace('#', '')
    product_name = product_name.replace('"""', '')
    product_name = product_name.strip()
    product_name = re.sub(r'[^a-zA-Z0-9\s]+', '', product_name)

    return product_name