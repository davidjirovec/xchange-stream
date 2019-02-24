function format(decimals) {
    var result = "";

    for (var i = 0; i < decimals.length; i++) {
        if (result) {
            result = result + ":";
        }
        result = result + decimals[i];
    }

    return result;
}
