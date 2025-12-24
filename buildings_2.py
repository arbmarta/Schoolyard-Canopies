"""
Explore FTP server structure before downloading
"""
from ftplib import FTP

FTP_HOST = 'dataserv.ub.tum.de'
FTP_USER = 'm1782307'
FTP_PASS = 'm1782307'

print("Exploring FTP Server Structure...\n")

try:
    ftp = FTP(FTP_HOST, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)

    print("✓ Connected successfully!\n")

    # Get current directory
    print(f"Current directory: {ftp.pwd()}")

    # List root directory
    print("\n--- Root Directory Contents ---")
    files = []
    ftp.retrlines('LIST', files.append)

    for item in files:
        print(item)

    # Save to file
    with open('outputs/ftp_structure.txt', 'w') as f:
        f.write("FTP Server Structure\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Current directory: {ftp.pwd()}\n\n")
        for item in files:
            f.write(f"{item}\n")

    print("\n\nStructure saved to: ftp_structure.txt")
    print("\nPlease check this file to understand how files are organized.")

    ftp.quit()

except Exception as e:
    print(f"Error: {e}")
    print("\nThis suggests:")
    print("1. FTP might be blocked by firewall")
    print("2. Credentials might be incorrect")
    print("3. Server might not allow FTP connections")