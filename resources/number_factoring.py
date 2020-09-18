def factoring_best_combo(my_num):
    best_combo = [0, 0, 0]
    start_j = 0 if my_num < 4 else 1
    for i in range(10):
        for j in range(start_j, i + 1):
            factor_one = i + 1
            factor_two = j + 1
            if factor_one * factor_two > best_combo[2] \
                and factor_one * factor_two <= my_num:
                best_combo[0] = factor_one
                best_combo[1] = factor_two
                best_combo[2] = factor_one * factor_two
    return best_combo[0], best_combo[1], best_combo[2]