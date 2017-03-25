

exports.GetVirtualCalcVal = function (row,HeaderRow,virtualcalc) {
	var curval;
	let stack = [];
	for (let step of virtualcalc) {
		if (step[0] == "readfrom") {
			curval = row[HeaderRow.indexOf(step[1])];
		}
		else if (step[0] == "push") {
			if (step[1] === undefined) {
				stack.push(curval);
			}
			else stack.push(step[1]);
			curval = "";
		}
		else if (step[0] == "concat") {
			if (curval=="") curval = stack.pop();
			curval = ""+stack.pop()+curval;
		}
		else if (step[0] == "leftpad") {
			if (curval=="") curval = stack.pop();
			let padamount = +curval;
			let padval = stack.pop();
			let str = stack.pop();
			if (str.length >= padamount) curval = str;
			else curval = padval.repeat(padamount-str.length)+str;
		}
		else if (step[0] == "replace_regexp") {
			if (curval=="") curval = stack.pop();
			let replacestr = curval;
			let regexstr = stack.pop();
			let curstr = stack.pop();
			if (curstr === undefined || curstr === null) curval = curstr;
			else {
				let re = new RegExp(regexstr);
				curval = curstr.replace(re,replacestr);
			}
		}
		else {
			throw("Unknown virtual calculation: "+step[0]);
		}
	}
	return curval;
};