from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class age_commuter(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.homecode_mapper,
                   reducer=self.homecode_reducer),
            MRStep(reducer=self.final_reducer)
            ]

    def homecode_mapper(self, key, line):
        cols = line.split(',')
        geocode = cols[0]
        if (len(geocode) < 15):
            return
        if len(cols) == 13:
            homecode = cols[1]
            workcode = cols[0]
            salary_range_1 = int(cols[6])
            salary_range_2 = int(cols[7])
            salary_range_3 = int(cols[8])
            yield homecode, (workcode, [salary_range_1, salary_range_2, salary_range_3], "od_salary_ranges")
        if len(cols) > 13:
            homecode = cols[0]
            salary_range_1 = int(cols[5])
            salary_range_2 = int(cols[6])
            salary_range_3 = int(cols[7])
            yield homecode, ("", [salary_range_1, salary_range_2, salary_range_3], "rac_salary_ranges")

    def homecode_reducer(self, key, values):
        commute = False
        value_list = list(values)

        # Checking for incomplete data
        if len(value_list) == 1:
            return

        # Getting total salary range counts based on residence area
        for (code, total_salary_ranges, keyword) in value_list:
            if (keyword == "rac_salary_ranges"):
                range_1_total = total_salary_ranges[0]
                range_2_total = total_salary_ranges[1]
                range_3_total = total_salary_ranges[2]
        
        range_1_cnt = 0
        range_2_cnt = 0
        range_3_cnt = 0

        # Populating actual counts using OD salary data
        for workcode, sals, name in value_list:
            # Checking if data is from OD
            if (name == "od_salary_ranges") and (workcode != key):
                range_1_cnt += sals[0]
                range_2_cnt += sals[1]
                range_3_cnt += sals[2]

        yield "Salary Range 1", (range_1_cnt, range_1_total)
        yield "Salary Range 2", (range_2_cnt, range_2_total)
        yield "Salary Range 3", (range_3_cnt, range_3_total)

    
    def final_reducer(self, key, values):
        commuters = 0
        totals = 0
        for (commuter, total) in values:
            commuters += commuter
            totals += total
        yield key, (commuters, totals, float(commuters/totals))


if __name__ == '__main__':
    age_commuter.run()
