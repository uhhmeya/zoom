#include "include/types.h"

vector<TransitionMetrics> transition_metrics(MAX_THREADS);
vector<HP_Slot> hp(MAX_THREADS);
vector<TB_slot> tb(MAX_KEYS);
vector<SpinMetrics> spin_metrics(MAX_THREADS);

thread_local vector<string*> retired_list;
thread_local int my_hp_index = -1;