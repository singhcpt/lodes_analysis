from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class age_commuter(MRJob):
    # define the steps for the map reduce job
    def steps(self):
        return [
            MRStep(mapper=self.age_mapper, reducer=self.age_reducer),
            MRStep(reducer=self.final_reducer)
        ]

    def age_mapper(self, key, line):
        split = line.split(',')

        geocode = split[0]
        if (len(geocode) < 15):
            return # filter out invalid geocodes

        if len(split) == 13: # OD table
            workcode = split[0]
            homecode = split[1]

            # query the values for each age range in OD table
            ages_lower = int(split[3]) # 29 and younger
            ages_middle = int(split[4]) # 30 - 54
            ages_upper = int(split[5]) # 55 and older

            yield homecode, (workcode, [ages_lower, ages_middle, ages_upper], "od_age_ranges")

        if len(split) > 13: # RAC table
            homecode = split[0]

            # query the values for each age range in RAC table
            ages_lower = int(split[2]) # 29 and younger
            ages_middle = int(split[3]) # 29 - 54
            ages_upper = int(split[4]) # 55 and older

            yield homecode, ('', [ages_lower, ages_middle, ages_upper], "rac_age_ranges")

    def age_reducer(self, key, value):
        commute = False

        # create variables for age range sums
        ages_lower_sum, ages_middle_sum, ages_upper_sum = 0;

        value_list = list(value)

        # check for incomplete data
        if len(value_list) == 1:
            return

        # aggregate totals of each range from RAC table
        for (area, age_range, keyword) in value_list:
            if (keyword == 'rac_age_ranges'):
                ages_lower_total = age_range[0]
                ages_middle_total = age_range[1]
                ages_upper_total = age_range[2]

        # sum the total people in each age range based on OD age data
        for (area, age_range, keyword) in value_list:
            # verifies we are in the OD table and that the person is a commuter so we only focus on that data
            if (keyword == 'od_age_ranges' and area[0:11] != key[0:11]):
                ages_lower_sum += age_range[0]
                ages_middle_sum += age_range[1]
                ages_upper_sum += age_range[2]

        # yield all the values for the different age ranges
        yield "ages_lower", (ages_lower_sum, ages_lower_total)
        yield "ages_middle", (ages_middle_sum, ages_middle_total)
        yield "ages_upper", (ages_upper_sum, ages_upper_total)

    def final_reducer(self, key, value):
        commmuter_total, age_total = 0

        # find complete sums of ALL commuters per age range and totals people per age range
        for (commuter, total) in value:
            commuter_total += commuter
            age_total += total

        # yield the age range, total number of commuters in the range, total people in the range, and ratio of commuters to total people in the range
        yield key, (commuter_total, age_total, float(commuter_total/age_total))

    if __name__ == '__main__':
        age_commuter.run()
