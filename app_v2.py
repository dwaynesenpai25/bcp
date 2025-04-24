from tabs.bcp_env1 import BCPAutomationE1
from tabs.bcp_env2 import BCPAutomationE2
from tabs.bcp_env3 import BCPAutomationE3

def main():
    try:
        print("Starting BCPAutomationE1...")
        bcpe1 = BCPAutomationE1()
        bcpe1.display()
        print("BCPAutomationE1 completed.")
    except Exception as e:
        print(f"BCPAutomationE1 failed: {e}")

    try:
        print("Starting BCPAutomationE2...")
        bcpe2 = BCPAutomationE2()
        bcpe2.display()
        print("BCPAutomationE2 completed.")
    except Exception as e:
        print(f"BCPAutomationE2 failed: {e}")


    try:
        print("Starting BCPAutomationE3...")
        bcpe3 = BCPAutomationE3()
        bcpe3.display()
        print("BCPAutomationE3 completed.")
    except Exception as e:
        print(f"BCPAutomationE3 failed: {e}")

if __name__ == "__main__":
    main()