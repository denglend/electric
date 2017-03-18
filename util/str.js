exports.SafeToLowerCase = function (str) {
	if (typeof str !== "string") return str;
	else return str.toLowerCase();
};