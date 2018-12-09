package Interfaces;

import services.ScheduledRequest;

public interface CompletionCallback {
	public void onSuccess(ScheduledRequest scheduled);
	public void onFailure(ScheduledRequest scheduled);
}
