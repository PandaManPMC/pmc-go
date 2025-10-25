package at

// SQLLimitCondition 设置 SQL 的 Limit 条件
// pageIndex 页码，从 1 开始。
// pageSize 页条目数
func SQLLimitCondition(condition map[string]interface{}, pageIndex, pageSize int) {
	condition[CondPageIndex] = pageIndex
	condition[CondPageSize] = pageSize
}

// SQLLimitMaxCondition 设置 SQL 的 Limit 条件。条目数默认是 0 至 2000。
func SQLLimitMaxCondition(condition map[string]interface{}) {
	SQLLimitCondition(condition, 1, 2000)
}

// SQLLimitMinCondition 设置 SQL 的 Limit 条件。条目数默认是 0 至 1，查询一条。
func SQLLimitMinCondition(condition map[string]interface{}) {
	SQLLimitCondition(condition, 1, 1)
}

// SQLBeginTime 设置起始时间条件
func SQLBeginTime(condition map[string]interface{}, beginUnix uint64) {
	condition[CondBeginTime] = beginUnix
}

// SQLAESFirst 默认查询升序第一条（如果没有 CondORDERField 则默认 id）
func SQLAESFirst(condition map[string]interface{}) {
	if _, isOk := condition[CondORDERField]; !isOk {
		condition[CondORDERField] = "id"
	}
	condition[CondORDERType] = CondORDERTypeAES
	condition[CondPageIndex] = 1
	condition[CondPageSize] = 1
}

// SQLDESCFirst 默认查询降序第一条（如果没有 CondORDERField 则默认 id）
func SQLDESCFirst(condition map[string]interface{}) {
	if _, isOk := condition[CondORDERField]; !isOk {
		condition[CondORDERField] = "id"
	}
	condition[CondORDERType] = CondORDERTypeDESC
	condition[CondPageIndex] = 1
	condition[CondPageSize] = 1
}

// SQLTimeCondition 设置起始时间，结束时间条件
func SQLTimeCondition(condition map[string]interface{}, beginUnix uint64, endUnix uint64) {
	condition[CondBeginTime] = beginUnix
	condition[CondEndTime] = endUnix
}
