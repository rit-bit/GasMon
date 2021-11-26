namespace GasMon
{
    public class TimeReadingPair
    {
        public string Time { get; set; }
        public decimal Reading { get; set; }

        public TimeReadingPair(string time, decimal reading)
        {
            Time = time;
            Reading = reading;
        }
    }
}