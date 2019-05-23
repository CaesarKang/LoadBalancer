## This script is used to dispatch computation tasks among the hpc cluster
## by Caesar
## 15 Mar 2019

include(raw"\\unicorn3\CRI3\DeepSelect_Refresh\SelectRegressors_Code\dataGenV3.jl")
include(raw"\\unicorn3\CRI3\DeepSelect_Refresh\SelectRegressors_Code\ismember_CK.jl")
using HTTP, CSV, PyCall
push!(pyimport("sys")["path"], raw"\\unicorn3\CRI3\DeepSelect_Refresh\Variable_Selection_API")
@pyimport Email_Alert

### client_servers cluster
# global client_servers = [
# "crisifi-2hyt"]
global client_servers = [
"crisifi-2hyt",
"CRI-HPC02",
"CRI-HPC03",
"CRI-HPC04",
"CRI-HPC05",
"CRI-HPC06",
"CRI-HPC09",
"CRI-HPC10",
"CRI-HPC11",
"CRI-HPC12",
"CRI-HPC14",
"CRI-HPC15",
"CRI-HPC16",
"CRI-HPC17",
"CRI-HPC19",
"CRI-PC21",
"CRI-PC23",
"CRI-PC24",
"CRI-PC25",
"CRI-PC26",
"CRI-PC27",
"CRI-PC34",
"CRI-PC35",
"CRI-PC36",
"CRI-PC38",
"CRI-HPC07",
"CRI-HPC08",
"CRI-HPC13",
"CRI-HPC18",
"CRI-PC39"
]



function load_balancer_gen(task_chnl::RemoteChannel{Channel{Any}}, csvr_chnl::RemoteChannel{Channel{Any}}, urls::Array{String}, params_dict::Dict, task_keys::Array{String}, error_path::String, r::Dict=Dict())
    date = string(Dates.now())[1:10]
    counter = 0
    task_id = 0
    r = Dict()
    filename = deepcopy(params_dict[task_keys[2]])

    @sync while isready(task_chnl)
        @sync while isready(task_chnl)
            task_id = take!(task_chnl)
            if !isready(csvr_chnl)
                wait(csvr_chnl)
            end
            @async begin
                iHPC = take!(csvr_chnl)
                url1 = replace(urls[1], "iHPC", iHPC)
                task = deepcopy(task_id)

                #task_id_cp = deepcopy(task_id[1]) ## to make task_id become a local variable
                #srd = deepcopy(task_id[2])
                #params_dict = input_proc(params_dict,task_keys,task_id_cp, srd)

                task_id_cp = deepcopy(task_id)
                params_dict = input_proc(params_dict,filename,task_keys,task_id_cp)

                try
                    r["task_$task_id_cp"] = HTTP.request("POST", url1, [], JSON.json(params_dict))
                    r["task_$task_id_cp"] = JSON.parse(String(r["task_$task_id_cp"].body))
                    put!(csvr_chnl, r["task_$task_id_cp"]["host"])
                    if r["task_$task_id_cp"]["error"] == true
                        put!(task_chnl, task)
                        counter = error_alert(counter, error_path, date, task_id_cp, r)
                    end
                catch e
                    bt = catch_backtrace()
                    println("enter into catch")
                    put!(task_chnl, task)
                    msg = sprint(showerror, e, bt)
                    counter = error_alert(counter, error_path, date, task_id_cp, iHPC, msg)
                end
            end
            sleep(1)
        end
    end
    if counter >= 2 && isfile(error_path * "\\error_task-$date.txt")
        contents = "Reassigning tasks for errors occurred when running simulation!"
        Email_Alert.sendemail("[DS]Error_report", contents, "error_task-$date.txt")
    end
    return r
end


function check_pc_status(client_servers::Array{String}, test_url::String)

    csvr_chnl = RemoteChannel(()->Channel(100), 1)
    final_csvr_chnl = RemoteChannel(()->Channel(100), 1)

    for i in client_servers
        put!(csvr_chnl, i)
    end

    prepInput(ikeys, ivalues) = JSON.json(Dict(zip(ikeys, ivalues)))
    ikeys = ["x", "y"]
    ivalues = [1, 1]
    paras_d_sim = prepInput(ikeys, ivalues)

    while isready(csvr_chnl)
        iHPC = take!(csvr_chnl)
        try
            test_url = replace(test_url, "iHPC", iHPC)
            r1 = HTTP.request("POST", test_url, [], paras_d_sim)
            res1 = JSON.parse(String(r1.body))
            if !res1["error"]
                put!(final_csvr_chnl, iHPC)
            end
        catch e
            bt = catch_backtrace()
            msg = sprint(showerror, e, bt)
            print("Error on $iHPC:\n", msg)
        end
    end

    return final_csvr_chnl
end

Sys.free_memory()/2^23
Sys.CPU_CORES


# function input_proc(params_dict,task_keys,task_id_cp, srd)
#
#     params_dict[task_keys[1]] = task_id_cp
#     params_dict[task_keys[2]] = srd
#
#     return params_dict
# end

function input_proc(params_dict,filename,task_keys,task_id_cp)

    params_dict[task_keys[1]] = task_id_cp
    filename_new = replace(filename, r"data.csv", "data_$task_id_cp.csv")
    params_dict[task_keys[2]] = filename_new

    return params_dict
end

function error_alert(counter, error_path, date, task_id_cp, r)
    open(error_path * "\\error_task-$date.txt", "a+") do file
        write(file, string(Dates.now())*"\n")
        write(file, "Errors occured when running task $task_id_cp on Host: " * r["task_$task_id_cp"]["host"])
        write(file, r["task_$task_id_cp"]["msg"])
        write(file, "\n")
    end
    counter += 1
    if counter == 1 && isfile(error_path * "\\error_task-$date.txt")
        contents = "Errors occurred when task $task_id_cp"
        Email_Alert.sendemail("[DS]Error_report", contents, "error_task-$date.txt")
    end
    return counter
end

function error_alert(counter, error_path, date, task_id_cp, iHPC, msg)
    open(error_path * "\\error_task-$date.txt", "a+") do file
        write(file, string(Dates.now())*"\n")
        write(file, "Errors occured when running task $task_id_cp on Host: " * iHPC)
        write(file, msg)
        write(file, "\n")
    end
    counter += 1
    if counter == 1 && isfile(error_path * "\\error_task-$date.txt")
        contents = "Errors occurred when task $task_id_cp"
        Email_Alert.sendemail("[DS]Error_report", contents, "error_task-$date.txt")
    end
    return counter
end
test_url = "http://iHPC:8920/trial"
final_csvr_chnl = check_pc_status(client_servers, test_url)
# while isready(final_csvr_chnl)
#     println(take!(final_csvr_chnl))
# end

while isready(csvr_chnl)
    println(take!(csvr_chnl))
end


error_path = raw"\\unicorn3\CRI3\DeepSelect_Refresh\SelectRegressors_Code\Error_reports"
urls = ["http://iHPC:8920/variable_selection"]

task_chnl = RemoteChannel(()->Channel(1e3), 1)
task_len = 600
# for i in 1:task_len
#     put!(task_chnl, (i, j))
# end
# srand(168)
rsd_trial = abs.(floor.(Int, rand(Int, task_len) ./ 1e12))
for (i, j) in zip(collect(1:task_len), rsd_trial)
    put!(task_chnl, (i, j))
end
rsd_trial[[1, 2, 22, 30, 34, 50]]
rsd_trial[23]
task_keys = ["sim", "srd"]
wt = 20
w_intercept = true
w_interact = false
cross_valid = false
common_run = false
start_regressor = 1
max_regressor = 20
n_params = 2000
working_directory = raw"\\unicorn3\CRI3\DeepSelect_Refresh\SelectRegressors_Code"
n_folds = 2
m_list = []
stats_flag = true
file_name = working_directory * "\\data_2.csv"
ikeys = ["w_intercept", "w_interact", "cross_valid", "common_run", "start_regressor", "max_regressor", "n_params",
        "working_directory", "n_folds", "m_list", "stats_flag", "file_name"]
ivalues = [w_intercept, w_interact, cross_valid, common_run, start_regressor, max_regressor, n_params,
          working_directory, n_folds, m_list, stats_flag, file_name]

params_dict = Dict(zip(ikeys, ivalues))


# r1 = HTTP.request("POST", test_url, [], paras_d_sim; readtimeout = 10)
# res1 = JSON.parse(String(r1.body))
# TEST
# final_csvr_chnl = RemoteChannel(()->Channel(100), 1)
# put!(final_csvr_chnl, "crisifi-2hyt")
# iHPC = take!(final_csvr_chnl)
# urls[1] = replace(urls[1], "iHPC", iHPC)
# params_dict[task_keys[1]] = 1
# r = Dict()
# r["task_1"] = HTTP.request("POST", urls[1], [], JSON.json(params_dict))
# r["task_1"] = JSON.parse(String(r["task_1"].body))
# r["task_1"]["msg"]
#
# JSON.parse(r["task_1"]["msg"])
# #####
r = load_balancer_gen(task_chnl, final_csvr_chnl, urls, params_dict, task_keys, error_path)


## data generation
# ss = 0
# ## Simulation Settings
# nofDataSets = 500
# # splSize = 200
# splSize = 1000
# noPs = 900
# Gps = 3
# noSelectedPs = 18
# β = [0.1, 0.2, 0.5, 0.7, 0.85, 1]
# # noSelectedPs = 9
# # β = [0.1, 0.5, 1]
# t = ones(Int, Gps)
# β = vcat(β[:, t']...)
# Rsqr_dataSets = 0.8
# # Rsqr_dataSets = 0.4
# Pl = Pu = 9
# kfolds = 2
# with_intercept = false
# create_interaction = false
# use_common_rseed = true
# Nparam = 1000
# selectedPs, dataSets, Rsqr_datasets_real = dataGenV3(nofDataSets, splSize, noPs, Gps, noSelectedPs, β, Pl, Pu, Rsqr_dataSets, kfolds, with_intercept, create_interaction, use_common_rseed)
#
# ss += 1
# path = raw"\\unicorn3\CRI3\DeepSelect_Refresh\SelectRegressors_Code\sim_data" * "_$ss"
# if !ispath(path)
#     mkpath(path)
# end
# R2 = []
# Ps = Int.(selectedPs["model_1"][1, :])
# for j = 1:nofDataSets
#     data = DataFrame(dataSets["set_$j"])
#     push!(R2, Rsqr_datasets_real["Rsqr_dataSets_$j"])
#
#     # rename!(data, :x1 => :y)
#     # model = DataFrame(selectedPs["model_$j"])
#     # CSV.write(raw"\\DIRAC\TeamData\DT\Caesar\DeepSelect_original\SelectRegressors_Code\model_" * "2.csv", model)
#     # CSV.write(raw"\\DIRAC\TeamData\DT\Caesar\DeepSelect_original\SelectRegressors_Code\data_" * "3.csv", data)
#     # CSV.write(raw"\\DIRAC\TeamData\DT\Caesar\DeepSelect_original\SelectRegressors_Code\data_" * "$j.csv", model)
#     CSV.write(path * "\\data_" * "$j.csv", data)
# end
# CSV.write(path * "\\Rsqr" * "$ss.csv", DataFrame(R2 = R2[1:end]))
# CSV.write(path * "\\Ps" * "$ss.csv", DataFrame(Ps = Ps))


ss = 0
ss += 1

test_url = "http://iHPC:8920/trial"
final_csvr_chnl = check_pc_status(client_servers, test_url)
jjj = 0
# while isready(final_csvr_chnl)
#     println(take!(final_csvr_chnl))
#     jjj += 1
# end
error_path = raw"\\unicorn3\CRI3\DeepSelect_Refresh\SelectRegressors_Code\Error_reports"
urls = ["http://iHPC:8920/variable_selection"]

task_chnl = RemoteChannel(()->Channel(1e3), 1)
task_len = 500

for i in 1:task_len
    put!(task_chnl, i)
end
task_keys = ["sim", "file_name"]

wt = 1

w_intercept = false
w_interact = false
cross_valid = true
common_run = false
start_regressor = 1
max_regressor = 20
n_params = 2000
working_directory = raw"\\unicorn3\CRI3\DeepSelect_Refresh\SelectRegressors_Code"
n_folds = 2
m_list = []
srd = 168
stats_flag = false
file_name = working_directory * "\\sim_data_$ss\\data.csv"
ikeys = ["w_intercept", "w_interact", "srd", "cross_valid", "common_run", "start_regressor", "max_regressor", "n_params",
        "working_directory", "n_folds", "m_list", "stats_flag", "file_name"]
ivalues = [w_intercept, w_interact, srd, cross_valid, common_run, start_regressor, max_regressor, n_params,
          working_directory, n_folds, m_list, stats_flag, file_name]
params_dict = Dict(zip(ikeys, ivalues))
r = load_balancer_gen(task_chnl, final_csvr_chnl, urls, params_dict, task_keys, error_path)

# final_csvr_chnl = RemoteChannel(()->Channel(100), 1)
# put!(final_csvr_chnl, "crisifi-2hyt")
# iHPC = take!(final_csvr_chnl)
# urls[1] = replace(urls[1], "iHPC", iHPC)
# params_dict[task_keys[2]] = replace(file_name, "data.csv", "data_1.csv")
# params_dict[task_keys[1]] = 1
# r = Dict()
# r["task_1"] = HTTP.request("POST", urls[1], [], JSON.json(params_dict))
r["task_1"] = JSON.parse(String(r["task_1"].body))


final = []
Precision = []
Ps_real = readcsv(path * "\\Ps$ss.csv")
Ps_real = Ps_real[2:end]
R2_real = readcsv(path * "\\Rsqr$ss.csv")
R2_real = R2_real[2:end]

for i = 1:nofDataSets
    res1 = readcsv(path * "\\result_$i.csv")
    Ps_ = eval(parse(res1[2]))[1]
    R2 = eval(parse(res1[2]))[2]
    final = [final; [Ps_' R2]]

end

stats = zeros(noSelectedPs)
count1 = count2 = 0
for i = 1:nofDataSets
    for ip in 1:noSelectedPs
        if in(Ps_real[ip], final[i, 1:9])
            stats[ip] = stats[ip] + 1
        end
    end
    if final[i, end] > Ps_real
        count1 += 1
    end
    if final[i, end] >= Ps_real
        count2 += 1
    end
end

reshape(stats ./ nofDataSets, 3, 3)
count1/nofDataSets
count2/nofDataSets

file_name1 = working_directory * "\\sim_data_$ss\\data_13.csv"
split(file_name1, r"data_\d+\.csv")[1]


test_url = "http://iHPC:8920/trial"
prepInput(ikeys, ivalues) = JSON.json(Dict(zip(ikeys, ivalues)))
ikeys = ["x", "y"]
ivalues = [1, 1]
paras_d_sim = prepInput(ikeys, ivalues)
iHPC = "CRI-HPC12"
test_url = replace(test_url, "iHPC", iHPC)
r1 = HTTP.request("POST", test_url, [], paras_d_sim)
res1 = JSON.parse(String(r1.body))
