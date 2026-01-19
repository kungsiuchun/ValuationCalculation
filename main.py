from generate_valuation import get_fmp_fragmented


def main():
    print("Hello from valuationcalculation!")


if __name__ == "__main__":
    main()
    inc_list = get_fmp_fragmented("income-statement", "AAPL")

    print(len(inc_list))