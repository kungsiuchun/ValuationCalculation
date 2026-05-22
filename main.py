from generate_valuation import get_latest_processed_quarter


def main():
    print("Hello from valuationcalculation!")


if __name__ == "__main__":
    main()
    latest_processed_quarter = get_latest_processed_quarter("TSM")

    print(latest_processed_quarter)