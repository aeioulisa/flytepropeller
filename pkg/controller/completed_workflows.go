package controller

import (
	"strconv"
	"time"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const controllerAgentName = "flyteworkflow-controller"
const workflowTerminationStatusKey = "termination-status"
const workflowTerminatedValue = "terminated"
const hourOfDayCompletedKey = "hour-of-day"

// This function creates a label selector, that will ignore all objects (in this case workflow) that DOES NOT have a
// label key=workflowTerminationStatusKey with a value=workflowTerminatedValue
func IgnoreCompletedWorkflowsLabelSelector() *v1.LabelSelector {
	return &v1.LabelSelector{
		MatchExpressions: []v1.LabelSelectorRequirement{
			{
				Key:      workflowTerminationStatusKey,
				Operator: v1.LabelSelectorOpNotIn,
				Values:   []string{workflowTerminatedValue},
			},
		},
	}
}

// Creates a new LabelSelector that selects all workflows that have the completed Label
func CompletedWorkflowsLabelSelector() *v1.LabelSelector {
	return &v1.LabelSelector{
		MatchLabels: map[string]string{
			workflowTerminationStatusKey: workflowTerminatedValue,
		},
	}
}

func SetCompletedLabel(w *v1alpha1.FlyteWorkflow, currentTime time.Time) {
	if w.Labels == nil {
		w.Labels = make(map[string]string)
	}
	w.Labels[workflowTerminationStatusKey] = workflowTerminatedValue
	w.Labels[hourOfDayCompletedKey] = strconv.Itoa(currentTime.Hour()+1)
}

func HasCompletedLabel(w *v1alpha1.FlyteWorkflow) bool {
	if w.Labels != nil {
		v, ok := w.Labels[workflowTerminationStatusKey]
		if ok {
			return v == workflowTerminatedValue
		}
	}
	return false
}

// Calculates a list of all the hours that should be deleted given the current hour of the day and the retentionperiod in hours
// Usually this is a list of all hours out of the 24 hours in the day - retention period - the current hour of the day
func CalculateHoursToDelete(retentionPeriodHours, currentHourOfDay int, gcInterval int) []string {
	hoursToDelete := make([]string, 0, gcInterval)
	if currentHourOfDay - retentionPeriodHours - gcInterval + 1 < 0 {
		for i := currentHourOfDay - retentionPeriodHours - gcInterval + 25; i <= currentHourOfDay - retentionPeriodHours + 24; i++ {
			hoursToDelete = append(hoursToDelete, strconv.Itoa(i))
		}
	}else {
		if currentHourOfDay - retentionPeriodHours < 24{
			for i := currentHourOfDay - retentionPeriodHours - gcInterval + 1; i <= currentHourOfDay - retentionPeriodHours; i++ {
				hoursToDelete = append(hoursToDelete, strconv.Itoa(i))
			}
		} else {

			for i := currentHourOfDay - retentionPeriodHours - gcInterval + 1; i <= currentHourOfDay - retentionPeriodHours; i++ {
				hoursToDelete = append(hoursToDelete, strconv.Itoa(i))
			}
		}
	}

	return hoursToDelete
}

// Creates a new selector that selects all completed workflows and workflows with completed hour label outside of the
// retention window
func CompletedWorkflowsSelectorOutsideRetentionPeriod(retentionPeriodHours int, currentTime time.Time,gcInterval int) *v1.LabelSelector {
	hoursToDelete := CalculateHoursToDelete(retentionPeriodHours, currentTime.Hour(), gcInterval)
	s := CompletedWorkflowsLabelSelector()
	s.MatchExpressions = append(s.MatchExpressions, v1.LabelSelectorRequirement{
		Key:      hourOfDayCompletedKey,
		Operator: v1.LabelSelectorOpIn,
		Values:   hoursToDelete,
	})
	return s
}
