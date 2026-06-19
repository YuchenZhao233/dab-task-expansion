# Gold Answers

## Query 1

For the derived Stack status labels, which L1 model family occurs most often for each status, strong and soft? Return status, L1 model family, and occurrence count.

status,l1_model,occurrence_count
strong,regression,78
soft,regression,36

## Query 2

Which problems have more configured solution versions than derived non-weak Stack occurrences across all steps? Return the problem card ID, problem name, solution-version count, and non-weak occurrence count.

problem_card_id,problem_name,solution_version_count,non_weak_occurrence_count
problem-card::hospital-mortality-prediction,Hospital Mortality Prediction,6,3
problem-card::iris,iris,7,4
problem-card::liver-disease-prediction,Liver disease prediction,5,1
problem-card::pumpkin-seeds,Pumpkin Seeds,4,0
problem-card::water-quality,water quality,3,1
problem-card::water-quality-2,water quality 2,3,1

## Query 3

Among regression problems, which five problems have the largest total positive Stack margin across strong steps? Return problem card ID, problem name, strong step count, and total margin rounded to 4 decimals.

problem_card_id,problem_name,strong_step_count,total_margin
problem-card::franck-hertz,Franck-Hertz,12,5.1836
problem-card::tunnel-diode,Tunnel diode,11,0.3879
problem-card::delaney-solubility,Delaney solubility,9,0.1570
problem-card::concrete,concrete,7,0.0682
problem-card::solar-power-generation,Solar Power Generation,16,0.0675

## Query 4

Among classification problems, which five have the most soft Stack outcomes? Sort by soft occurrence count descending, then average Stack test score descending, then problem name ascending. Return problem card ID, problem name, soft occurrence count, number of versions involved, and average Stack test score rounded to 4 decimals.

problem_card_id,problem_name,soft_occurrence_count,version_count,avg_stack_test
problem-card::kindey-stone-urine-analysis,kindey stone urine analysis,16,6,0.9932
problem-card::oil-spill,oil spill,8,4,0.9810
problem-card::smoke-detection-iot,smoke detection iot,5,4,1.0000
problem-card::survey-lung-cancer,survey lung cancer,5,3,0.9738
problem-card::lithium-ion-batteries,lithium ion batteries,4,2,1.0000

## Query 5

For each step, which non-Stack model most often ties the Stack model in soft outcomes? Return the step number, tied model label, and tie count.

step,model_label,tie_count
1,RFCE,9
2,RFCE,6
3,RFCE,9

## Query 6

Considering only strong Stack steps, which problems have nonzero feature-importance records for features marked as dropped by correlation? Return all such problem card IDs, problem names, contributing feature-record counts, and average importance rounded to 4 decimals.

problem_card_id,problem_name,feature_record_count,avg_importance
problem-card::solar-power-generation,Solar Power Generation,3,0.2943
problem-card::oil-spill,oil spill,104,0.0001
problem-card::pcos,PCOS,3,0.0000

## Query 7

Among resampled solution runs using a regression L1 family, which seven step instances have the highest Stack test score? Return step instance key, problem name, version, step, derived status, and Stack test score rounded to 4 decimals.

step_instance_key,problem_name,version,step,derived_status,stack_test_score
run::kindey-stone-urine-analysis::v003::stage-1,kindey stone urine analysis,3,1,soft,1.0000
run::kindey-stone-urine-analysis::v004::stage-1,kindey stone urine analysis,4,1,soft,1.0000
run::kindey-stone-urine-analysis::v004::stage-2,kindey stone urine analysis,4,2,soft,1.0000
run::kindey-stone-urine-analysis::v004::stage-3,kindey stone urine analysis,4,3,soft,1.0000
run::oil-spill::v003::stage-1,oil spill,3,1,soft,0.9931
run::oil-spill::v003::stage-2,oil spill,3,2,soft,0.9931
run::kindey-stone-urine-analysis::v003::stage-2,kindey stone urine analysis,3,2,weak,0.9879

## Query 8

For each problem type, which derived non-weak Stack status is most common? Return problem type, status, and count.

problem_type,status,occurrence_count
classification,soft,47
regression,strong,61

## Query 9

Which five solution versions have non-weak Stack outcomes in all three steps and the highest average Stack test score? Return run key, problem name, version, strong count, soft count, and average Stack test score rounded to 4 decimals.

run_key,problem_name,version,strong_count,soft_count,avg_stack_test
run::kindey-stone-urine-analysis::v004,kindey stone urine analysis,4,0,3,1.0000
run::kindey-stone-urine-analysis::v005,kindey stone urine analysis,5,0,3,1.0000
run::kindey-stone-urine-analysis::v006,kindey stone urine analysis,6,0,3,1.0000
run::kindey-stone-urine-analysis::v007,kindey stone urine analysis,7,0,3,1.0000
run::oil-spill::v005,oil spill,5,3,0,0.9885

## Query 10

Across strong Stack steps, which five step instances show the largest Stack train-test score gap? Return step instance key, problem name, version, step, L1 model family, Stack train score, Stack test score, and gap, with scores rounded to 4 decimals.

step_instance_key,problem_name,version,step,l1_model,stack_train,stack_test,gap
run::pss3e5::v004::stage-2,PSS3E5,4,2,regression,1.0000,0.6204,0.3796
run::pss3e5::v005::stage-2,PSS3E5,5,2,regression,1.0000,0.6449,0.3551
run::pss3e5::v004::stage-3,PSS3E5,4,3,regression,0.9939,0.6408,0.3531
run::pss3e5::v005::stage-1,PSS3E5,5,1,regression,0.9939,0.6531,0.3409
run::pss3e5::v005::stage-3,PSS3E5,5,3,regression,1.0000,0.6735,0.3265
