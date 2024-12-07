import win32com.client 

def get_installed_antivirus():
    try:
        wmi = win32com.client.GetObject("winmgmts:\\\\.\\root\\SecurityCenter2")
        products = wmi.ExecQuery("SELECT * FROM AntiVirusProduct")
        if products:
            print(dir(products[0]))

        antivirus_list = [product.displayName for product in products]
        return ", ".join(antivirus_list) if antivirus_list else "No antivirus detected"
    except Exception as e:
        return f"Failed to detect antivirus: {str(e)}"
    

print(get_installed_antivirus())
